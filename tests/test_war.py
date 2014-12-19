import war
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')


class RangeProcessor(war.ChunkProcesssor):
    def process_chunk(self, core, chunk):
        for i in range(core):
            yield i


class LineProcessor(war.ChunkProcesssor):
    def process_chunk(self, core, chunk):
        for line in chunk:
            yield 'cu' + line.strip() + 'cu'


dispatcher = war.TaskDispatcher()

task = war.InMemoryTask(RangeProcessor(), range(100), 4)
dispatcher.run(task)
print task.outputs()

task = war.FSTask(LineProcessor(), 'test.csv', 4)
dispatcher.run(task)
print task.outputs()