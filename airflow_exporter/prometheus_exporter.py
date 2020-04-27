from sqlalchemy import func, text, and_

from flask import Response
from flask_admin import BaseView, expose

from airflow.plugins_manager import AirflowPlugin
from airflow import settings
from airflow.settings import Session
from airflow.models import TaskInstance, DagModel, DagRun, DagBag
from airflow.utils.state import State

from prometheus_client import generate_latest, REGISTRY
from prometheus_client.core import GaugeMetricFamily

from contextlib import contextmanager

import itertools

import pytz
import datetime as dt
from croniter import croniter

cron_presets = {
    '@hourly': '0 * * * *',
    '@daily': '0 0 * * *',
    '@weekly': '0 0 * * 0',
    '@monthly': '0 0 1 * *',
    '@yearly': '0 0 1 1 *',

} 


@contextmanager
def session_scope(session):
    try:
        yield session
    finally:
        session.close()

def get_dag_state_info():
    dag_status_query = Session.query(
        DagRun.dag_id, DagRun.state, func.count(DagRun.state).label('count')
    ).group_by(DagRun.dag_id, DagRun.state).subquery()

    return Session.query(
        dag_status_query.c.dag_id, dag_status_query.c.state, dag_status_query.c.count,
        DagModel.owners
    ).join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id).all()


def get_task_state_info():
    task_status_query = Session.query(
        TaskInstance.dag_id, TaskInstance.task_id,
        TaskInstance.state, func.count(TaskInstance.dag_id).label('value')
    ).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state).subquery()

    return Session.query(
        task_status_query.c.dag_id, task_status_query.c.task_id, 
        task_status_query.c.state, task_status_query.c.value, DagModel.owners
    ).join(DagModel, DagModel.dag_id == task_status_query.c.dag_id).order_by(task_status_query.c.dag_id).all()


def get_dag_duration_info():
    driver = Session.bind.driver 
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
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
            )
            .join(DagModel, DagModel.dag_id == DagRun.dag_id,)
            .filter(
                DagModel.is_active == True,  
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


def get_dag_scheduler_delay():
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
                DagRun.state 

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
    
    def describe(self):
        return []

    def collect(self):
        
        #Task metrics
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

        driver = Session.bind.driver 
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


        # Scheduler Metrics
        dag_scheduler_delay = GaugeMetricFamily(
            "airflow_dag_scheduler_delay",
            "Airflow DAG scheduling delay",
            labels=["dag_id", "execution_date","schedule_interval","start_date","scheduled_start_date"],
        )
        dag_execution_delay = GaugeMetricFamily(
            "airflow_dag_execution_delay",
            "Airflow DAG execution delay",
            labels=["dag_id","now" ,"start_date","next_start_date","state"],
        )

        now = dt.datetime.utcnow()
        for dag in get_dag_scheduler_delay():
            if croniter.is_valid(dag.schedule_interval):
                c_start = croniter(dag.schedule_interval, dag.execution_date)
                c_next_start= croniter(dag.schedule_interval, dag.start_date)
            else:
                c_start = croniter(cron_presets.get(dag.schedule_interval), dag.execution_date)
                c_next_start= croniter(cron_presets.get(dag.schedule_interval), dag.start_date)
                    
            scheduled_start_date = c_start.get_next(dt.datetime)
            next_start_date = c_next_start.get_next(dt.datetime)

            dag_scheduling_delay_value = (
                dag.start_date - scheduled_start_date
            ).total_seconds()
            dag_scheduler_delay.add_metric(
                [dag.dag_id, str(dag.execution_date.date),dag.schedule_interval, str(dag.start_date.date), str(scheduled_start_date) ], dag_scheduling_delay_value
            )

            if next_start_date.replace(tzinfo=pytz.UTC) < now.replace(tzinfo=pytz.UTC) :
                dag_execution_delay_value= (now.replace(tzinfo=pytz.UTC) - next_start_date.replace(tzinfo=pytz.UTC) ).total_seconds()
            else :
                dag_execution_delay_value= 0
            dag_execution_delay.add_metric(
                [dag.dag_id, str(now),str(dag.start_date) ,str(next_start_date), dag.state  ] , dag_execution_delay_value
            )

        yield dag_execution_delay  
        yield dag_scheduler_delay


REGISTRY.register(MetricsCollector())

if settings.RBAC:
    from flask_appbuilder import BaseView as FABBaseView, expose as FABexpose
    class RBACMetrics(FABBaseView):
        route_base = "/admin/metrics/"
        @FABexpose('/')
        def list(self):
            return Response(generate_latest(), mimetype='text')


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