from sqlalchemy import func
from sqlalchemy import text
from sqlalchemy import and_

from flask import Response
from flask_admin import BaseView, expose

from airflow.plugins_manager import AirflowPlugin
from airflow import settings
from airflow.settings import Session
from airflow.models import TaskInstance, DagModel, DagRun, DagBag
from airflow.utils.state import State

# Importing base classes that we need to derive
from prometheus_client import generate_latest, REGISTRY
from prometheus_client.core import GaugeMetricFamily

from contextlib import contextmanager

import itertools

@contextmanager
def session_scope(session):
    """Provide a transactional scope around a series of operations."""
    try:
        yield session
    finally:
        session.close()

def get_dag_state_info():
    '''get dag info
    :return dag_info
    '''
    dag_status_query = Session.query(
        DagRun.dag_id, DagRun.state, func.count(DagRun.state).label('count')
    ).group_by(DagRun.dag_id, DagRun.state).subquery()

    return Session.query(
        dag_status_query.c.dag_id, dag_status_query.c.state, dag_status_query.c.count,
        DagModel.owners
    ).join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id).all()


def get_task_state_info():
    '''get task info
    :return task_info
    '''
    task_status_query = Session.query(
        TaskInstance.dag_id, TaskInstance.task_id,
        TaskInstance.state, func.count(TaskInstance.dag_id).label('value')
    ).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state).subquery()

    return Session.query(
        task_status_query.c.dag_id, task_status_query.c.task_id, 
        task_status_query.c.state, task_status_query.c.value, DagModel.owners
    ).join(DagModel, DagModel.dag_id == task_status_query.c.dag_id).order_by(task_status_query.c.dag_id).all()

def get_successful_dag_duration_info():
    """Duration of successful DAG Runs."""
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
                DagRun.state == State.SUCCESS,
                DagRun.end_date.isnot(None),
                text("execution_date > NOW() - interval \'14 days\'"),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )

        dag_start_dt_query = (
            session.query(
                max_execution_dt_query.c.dag_id,
                max_execution_dt_query.c.max_execution_dt.label(
                    "execution_date"
                ),
                func.min(TaskInstance.start_date).label("start_date"),
            )
            .join(
                TaskInstance,
                and_(
                    TaskInstance.dag_id == max_execution_dt_query.c.dag_id,
                    (
                        TaskInstance.execution_date
                        == max_execution_dt_query.c.max_execution_dt
                    ),
                ),
            )
            .group_by(
                max_execution_dt_query.c.dag_id,
                max_execution_dt_query.c.max_execution_dt,
            )
            .subquery()
        )

        return (
            session.query(
                dag_start_dt_query.c.dag_id,
                dag_start_dt_query.c.start_date,
                DagRun.end_date,
            )
            .join(
                DagRun,
                and_(
                    DagRun.dag_id == dag_start_dt_query.c.dag_id,
                    DagRun.execution_date
                    == dag_start_dt_query.c.execution_date,
                ),
            )
            .all()
        )


def get_dag_duration_info():
    '''get duration of currently running DagRuns
    :return dag_info
    '''
    driver = Session.bind.driver # pylint: disable=no-member
    durations = {
        'pysqlite': func.julianday(func.current_timestamp() - func.julianday(DagRun.start_date)) * 86400.0,
        'mysqldb':  func.timestampdiff(text('second'), DagRun.start_date, func.now()),
        'pyodbc': func.sum(func.datediff(text('second'), DagRun.start_date, func.now())),
        'default':  func.now() - DagRun.start_date
    }
    duration = durations.get(driver, durations['default'])

    return Session.query(
        DagRun.dag_id,
        func.max(duration).label('duration')
    ).group_by(
        DagRun.dag_id
    ).filter(
        DagRun.state == State.RUNNING
    ).all()

def get_task_duration_info():
    """Duration of successful tasks in seconds."""
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id,)
            .filter(
                DagModel.is_active == True,  # noqa
                DagModel.is_paused == False,
                DagRun.state == State.SUCCESS,
                DagRun.end_date.isnot(None),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )

        return (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.start_date,
                TaskInstance.end_date,
                TaskInstance.execution_date,
            )
            .join(
                max_execution_dt_query,
                and_(
                    (TaskInstance.dag_id == max_execution_dt_query.c.dag_id),
                    (
                        TaskInstance.execution_date
                        == max_execution_dt_query.c.max_execution_dt
                    ),
                ),
            )
            .filter(
                TaskInstance.state == State.SUCCESS,
                TaskInstance.start_date.isnot(None),
                TaskInstance.end_date.isnot(None),
            )
            .all()
        )

def get_dag_labels(dag_id):
    # reuse airflow webserver dagbag
    if settings.RBAC:
        from airflow.www_rbac.views import dagbag
    else:
        from airflow.www.views import dagbag

    dag = dagbag.get_dag(dag_id)

    if dag is None:
        return [], []
    
    labels = dag.params.get('labels')

    if labels is None:
        return [], []
    
    return list(labels.keys()), list(labels.values())

######################
# Scheduler Related Metrics
######################


def get_dag_schedule_delays():
    """Schedule delay for dags in seconds"""

    now = dt.datetime.now().replace(tzinfo=pytz.UTC)
    week_ago = now - dt.timedelta(weeks=1)

    with session_scope(Session) as session:
        max_id_query = (
            session.query(func.max(DagRun.id))
            .filter(DagRun.execution_date.between(week_ago, now))
            .group_by(DagRun.dag_id)
            .subquery()
        )

        return (
            session.query(
                DagModel.dag_id,
                DagModel.schedule_interval,
                DagRun.execution_date,
                DagRun.start_date,
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id)
            .filter(
                DagModel.is_active == True,
                DagModel.is_paused == False,
                DagModel.schedule_interval.isnot(None),
                DagRun.id.in_(max_id_query),
            )
            .all()
        )



class MetricsCollector(object):
    '''collection of metrics for prometheus'''

    def describe(self):
        return []

    def collect(self):
        '''collect metrics'''

        # Task metrics
        # Each *MetricFamily generates two lines of comments in /metrics, try to minimize noise 
        # by creating new group for each dag
        task_info = get_task_state_info()
        for dag_id, tasks in itertools.groupby(task_info, lambda x: x.dag_id):
            k, v = get_dag_labels(dag_id)

            t_state = GaugeMetricFamily(
                'airflow_task_status',
                'Shows the number of task starts with this status',
                labels=['dag_id', 'task_id', 'owner', 'status'] + k
            )
            for task in tasks:
                t_state.add_metric([task.dag_id, task.task_id, task.owners, task.state or 'none'] + v, task.value)
            
            yield t_state

        task_duration = GaugeMetricFamily(
            "airflow_task_duration",
            "Duration of successful tasks in seconds",
            labels=["task_id", "dag_id", "execution_date"],
        )
        for task in get_task_duration_info():
            task_duration_value = (
                task.end_date - task.start_date
            ).total_seconds()
            task_duration.add_metric(
                [task.task_id, task.dag_id, str(task.execution_date.date())],
                task_duration_value,
            )
        yield task_duration

        # Dag Metrics
        dag_info = get_dag_state_info()
        for dag in dag_info:
            k, v = get_dag_labels(dag.dag_id)

            d_state = GaugeMetricFamily(
                'airflow_dag_status',
                'Shows the number of dag starts with this status',
                labels=['dag_id', 'owner', 'status'] + k
            )
            d_state.add_metric([dag.dag_id, dag.owners, dag.state] + v, dag.count)
            yield d_state

        # DagRun metrics
        driver = Session.bind.driver # pylint: disable=no-member
        for dag in get_dag_duration_info():
            k, v = get_dag_labels(dag.dag_id)

            dag_duration = GaugeMetricFamily(
                'airflow_dag_run_duration',
                'Maximum duration of currently running dag_runs for each DAG in seconds',
                labels=['dag_id'] + k
            )
            if driver == 'mysqldb' or driver == 'pysqlite':
                dag_duration.add_metric([dag.dag_id] + v, dag.duration)
            else:
                dag_duration.add_metric([dag.dag_id] + v, dag.duration.seconds)
            yield dag_duration

        successful_dag_duration = GaugeMetricFamily(
            "airflow_successful_dag_run_duration",
            "Duration of successful dag_runs in seconds",
            labels=["dag_id"],
        )
        for dag in get_successful_dag_duration_info():
            successful_dag_duration_value = (
                dag.end_date - dag.start_date
            ).total_seconds()
            successful_dag_duration.add_metric([dag.dag_id], successful_dag_duration_value)
        yield successful_dag_duration


         # Scheduler Metrics
        dag_scheduler_delay = GaugeMetricFamily(
            "airflow_dag_scheduler_delay",
            "Airflow DAG scheduling delay",
            labels=["dag_id","execution_date","start_date","interval_bucket"],
        )
        for dag in get_dag_schedule_delays():
            if dag.schedule_interval is not None:
                c = croniter(dag.schedule_interval, dag.execution_date)
                planned_start_date = c.get_next(dt.datetime)

                interval = (
                    planned_start_date - dag.execution_date
                ).total_seconds() / 3600.0
                if interval <= 1:
                    interval_bucket = "<1h"
                elif interval > 1 and interval <= 6:
                    interval_bucket = "1-6h"
                else:
                    interval_bucket = ">6h"

                dag_schedule_delay = (
                    dag.start_date - planned_start_date
                ).total_seconds()
                airflow_dag_schedule_delay.add_metric(
                    [dag.dag_id, dag.execution_date, dag.start_date, interval_bucket], dag_schedule_delay
                )
        yield airflow_dag_schedule_delay



REGISTRY.register(MetricsCollector())

if settings.RBAC:
    from flask_appbuilder import BaseView as FABBaseView, expose as FABexpose
    class RBACMetrics(FABBaseView):
        route_base = "/admin/metrics/"
        @FABexpose('/')
        def list(self):
            return Response(generate_latest(), mimetype='text')


    # Metrics View for Flask app builder used in airflow with rbac enabled
    RBACmetricsView = {
        "view": RBACMetrics(),
        "name": "metrics",
        "category": "Admin"
    }

    www_views = []
    www_rbac_views = [RBACmetricsView]

else:
    class Metrics(BaseView):
        @expose('/')
        def index(self):
            return Response(generate_latest(), mimetype='text/plain')

    www_views = [Metrics(category="Admin", name="Metrics")]
    www_rbac_views = []


class AirflowPrometheusPlugins(AirflowPlugin):
    '''plugin for show metrics'''
    name = "airflow_prometheus_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = www_views
    flask_blueprints = []
    menu_links = []
    appbuilder_views = www_rbac_views
    appbuilder_menu_items = []