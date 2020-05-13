import datetime
from datetime import timedelta
import time

import boto3
import schedule

import settings
from exceptions import GettingCPUDataException

EC2 = 'ec2'
CLOUDWATCH = 'cloudwatch'


class AWSClient(object):
    """
    Manages an AWS session and simplifies the connection to AWS entities
    """
    def __init__(self, access_key, secret_key, default_region=None, auto_connect=True):
        self.access_key = access_key
        self.secret_key = secret_key
        self.session = None
        self.default_region = default_region
        self._connect() if auto_connect else None

    def _connect(self):
        """
        Creates an open session with the AWS account
        """
        self.session = boto3.Session(aws_access_key_id=self.access_key,
                                     aws_secret_access_key=self.secret_key,
                                     region_name=self.default_region)

    def get_aws_client(self, service_name, **kwargs):
        """
        :param service_name: the requested aws service
        :return: an open session to that service
        """
        return self.session.client(service_name, **kwargs)

    def get_aws_resource(self, resource_name, **kwargs):
        """
        :param resource_name: the requested aws resource
        :return: an open session to that service
        """
        return self.session.resource(resource_name, **kwargs)


def get_instance_metric(instance_id, metric_name, connection):
    """

    :param instance_id: an EC2 instance's id
    :param metric_name: the name of the metric we want to fetch
    :param connection: a resource connection to the aws account
    :return: a client for a specific instance and metric
    """
    metrics_client = connection.get_metric_statistics(
        Namespace='AWS/EC2',
        MetricName=metric_name,
        Dimensions=[
            {
                'Name': 'InstanceId',
                'Value': instance_id
            },
        ],
        StartTime=datetime.datetime.now() - timedelta(hours=settings.METRIC_TIME_DELTA),
        EndTime=datetime.datetime.now(),
        Period=settings.METRIC_TIME_PERIOD,
        Statistics=[
            'Average',
        ],
        Unit='Percent'
    )

    return metrics_client


def get_instance_cpu(metrics_client):
    """
    :param metrics_client: a client connection to aws metrics
    :return: a specific instance's CPU level if available
    """
    if metrics_client['Datapoints']:
        for metric in metrics_client['Datapoints']:
            if 'Average' in metric:
                return float(metric['Average'])
            else:
                raise Exception
    else:
        raise Exception


def is_instance_in_debug_mode(instance):
    """
    :param instance: a specific instance
    :return: whether the instance has a debug tag or not
    """
    try:
        for tag in instance.tags:
            if tag['Key'] == 'Debug' and tag['Value'] == 'yes':
                print(f"no need to delete {instance.id} Debug tag is on")
                return True
        return False
    except TypeError:
        return False


def run_instance_check_flow():
    """
    Terminates all instances with low CPU level and without Debug tag
    """
    aws_client = AWSClient(settings.ACCESS_KEY, settings.SECRET_KEY, settings.DEFAULT_REGION)
    cloudwatch_client = aws_client.get_aws_client(CLOUDWATCH)
    ec2_client = aws_client.get_aws_resource(EC2)

    print('------RUN STARTED------')

    for instance in ec2_client.instances.all():
        print(f'Start checking instance {instance.id}')
        if instance.state['Name'] == 'running':
            try:
                instance_cpu = get_instance_cpu(get_instance_metric(instance.id, 'CPUUtilization', cloudwatch_client))
            except Exception:
                raise GettingCPUDataException(instance.id)

            if not is_instance_in_debug_mode(instance) and instance_cpu < settings.INSTANCE_CPU_THRESHOLD_PERCENT:
                print(f'Terminating instance {instance.id}, instance CPU Usage: {instance_cpu}%')
                instance.terminate()
        print(f'Finished checking instance {instance.id}')

    print('------RUN FINISHED------')


if __name__ == '__main__':
    for time_schedule in settings.TIME_SCHEDULE:
        schedule.every().day.at(time_schedule).do(run_instance_check_flow)

    while True:
        schedule.run_pending()
        time.sleep(1)
