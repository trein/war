import war
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')


class DummyTask(war.InMemoryTask):
    def _process_chunk(self, core, chunk):
        for i in range(core):
            yield i


class DummyFSTask(war.FSTask):
    def _process_chunk(self, core, chunk):
        for line in chunk:
            yield 'cu' + line.strip() + 'cu'


dispatcher = war.TaskDispatcher()

task = DummyTask(range(100), 4)
dispatcher.run(task)
print task.outputs()

task = DummyFSTask('test.csv', 4)
dispatcher.run(task)
print task.outputs()