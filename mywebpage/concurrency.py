import asyncio

async def run_cpu_task(func, *args, cpu_pool, cpu_sem):
    async with cpu_sem:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(cpu_pool, func, *args)

