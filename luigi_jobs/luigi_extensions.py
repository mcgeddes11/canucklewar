import luigi
from utils import build_utils
from multiprocessing import Manager

# superclass that defines withConfig() method
class ConfigurableTask(luigi.Task):

    def __init__(self):
        # Create a job parameter to be filled later with the job config for every task
        self.job = None
        super(ConfigurableTask, self).__init__()
