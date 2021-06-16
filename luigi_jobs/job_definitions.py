from __future__ import print_function

import sys
from copy import deepcopy
import hashlib
import json
import utils.build_utils as buildutils
from luigi_jobs.luigi_extensions import ConfigurableTask
import logging
import abc
import os
import luigi

from luigi_jobs.build_database_tasks import TaskOne

# config locations
JOB_CONFIG_PATH = buildutils.absolute_path_from_project_root('configs/job.yaml')
CONFIG_HASH_LENGTH = 9
LOGGER_NAME = 'luigi-interface'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(module)s - %(message)s'
PROCESS_LOG_FILE_NAME = "build_process.log"
LOGGER = logging.getLogger(LOGGER_NAME)



class CustomLuigiJob:

    def __init__(self, config_file_path):
        self.job_config = self.get_job_configuration(config_file_path)
        self.tasks = None

    @abc.abstractmethod
    def create_task_list(self):
        tasks = []
        self.tasks = tasks


    def setup_logging(self):
        # luigi overrides log level during setup and adds its own handler
        formatter = logging.Formatter(fmt=LOG_FORMAT)
        logger = logging.getLogger(LOGGER_NAME)
        LOGFILE_NAME = os.path.join(self.job_config["data_repository"], "job_log.log")
        file_handler = logging.FileHandler(LOGFILE_NAME)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.propagate = False # prevents duplicate logging in the console

    def create_output_folder_structure(self, tasks):
        # Look over every output in our task structure and create the appropriate folders. Saves doing it in the run method
        # of every f-ing task
        for t in self.tasks:
            outputs = t.output()
            for k in outputs.keys():
                pathname = outputs[k].path
                if not os.path.exists(pathname):
                    buildutils.create_folder(pathname)

    def get_job_configuration(self, config_file_path):
        default_job_config = buildutils.load_yaml_config(config_file_path)
        job_config = buildutils.override_config_from_environment(default_job_config)
        if "config_id" not in job_config.keys():
            config_string = json.dumps(job_config).encode('utf-8')
            job_config['config_id'] = hashlib.sha256(config_string).hexdigest()[:CONFIG_HASH_LENGTH]
        print('Config ID: {}\n'.format(job_config['config_id']))
        return job_config

    def init_data_repository(self):
        absolute_data_repository = buildutils.absolute_path_from_project_root(self.job_config["data_repository"])
        data_repo_path = os.path.join(absolute_data_repository, self.job_config["config_id"])
        if not os.path.exists(data_repo_path):
            buildutils.create_folder(os.path.join(data_repo_path))
        self.job_config["data_repository"] = data_repo_path

    def run_job(self) -> bool:
        self.init_data_repository()
        self.setup_logging()
        self.job_config["job_start"] = buildutils.utc_now()
        LOGGER.info('Config ID: {}'.format(self.job_config['config_id']))
        LOGGER.info('Interim Directory: {}'.format(self.job_config["data_repository"]))
        ConfigurableTask.set_config(self.job_config)
        self.create_task_list()
        self.create_output_folder_structure(self.tasks)
        luigi.build(
            self.tasks,
            local_scheduler=self.job_config["local_scheduler"],
            workers=self.job_config["luigi_worker_count"],
            log_level=self.job_config["log_level"])

        success = all([task.complete() for task in self.tasks])
        return success


class BuildDatabaseJob(CustomLuigiJob):

    def create_task_list(self):
        tasks = []
        tasks.append(TaskOne())
        self.tasks = tasks


