from luigi_jobs.job_definitions import BuildDatabaseJob
from utils import build_utils

JOB_CONFIG_PATH = build_utils.absolute_path_from_project_root('config/build_database.yaml')
LOG_FILE_PATH = build_utils.absolute_path_from_project_root('logfile.log')


if __name__ == "__main__":
    job = BuildDatabaseJob(JOB_CONFIG_PATH)
    job.run_job()