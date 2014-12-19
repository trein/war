# War
Simple multiprocessing library for Python

## Usage
Processing data structure that fits in memory:

```python
import war

class CounterProcessor(war.ChunkProcesssor):
    def process_chunk(self, core, chunk):
        for i in chunk:
            yield i + 1


cores = 4
huge_input_list = range(1000000)
task = war.InMemoryTask(RangeProcessor(), huge_input_list, cores)

war.run(task)

print task.outputs()
```

