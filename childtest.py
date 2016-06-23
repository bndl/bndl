import sys
import asyncio

args = ('-c', 'print("hello world")')



@asyncio.coroutine
def run_subproc():
    yield from asyncio.create_subprocess_exec(
        sys.executable, *args,
        stderr=asyncio.subprocess.STDOUT
    )

loop = asyncio.get_event_loop()
loop.run_until_complete(run_subproc())
print('done')
