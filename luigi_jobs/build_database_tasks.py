from luigi_jobs.luigi_extensions import ConfigurableTask
import luigi
import os
from scraping.scraping_functions import get_schedule
from utils.build_utils import save_data, load_data




class TaskOne(ConfigurableTask):

    def run(self):
        placeholder_data = get_schedule("20172018")
        save_data(placeholder_data, self.output()["TaskOneOutput"].path)

    def output(self):
        return {"TaskOneOutput": luigi.LocalTarget(os.path.join(self.job["data_repository"], "some_filename.json"))}