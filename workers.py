import asyncio
from abc import ABC, abstractmethod
from config import config, logger
from time import perf_counter
from typing import List, Optional


DOUCH_CHEF_WORK_TIME: int = config["DOUCH_CHEF_WORK_TIME"]
TOPPING_CHEF_WORK_TIME: int = config["TOPPING_CHEF_WORK_TIME"]
OVEN_WORK_TIME: int = config["OVEN_WORK_TIME"]
WAITER_WORK_TIME: int = config["WAITER_WORK_TIME"]


class Worker(ABC):
    """
    Base class of each of the workers types
    """
    def __init__(self, idx: int, in_queue: asyncio.Queue, out_queue: Optional[asyncio.Queue] = None):
        self._id = idx
        self.in_queue = in_queue
        self.out_queue = out_queue

    @abstractmethod
    async def job(self) -> None:
        pass


class DouchChef(Worker):
    def __init__(self, *args):
        super().__init__(*args)

    async def job(self) -> None:
        while True:
            try:
                order_data = await self.in_queue.get()
                order_id = order_data.order_id

                start = perf_counter()
                order_data.start_time = start
                logger.info(f"Dough chef #{self._id} starting pizza #{order_id}, time = {start}")
                await asyncio.sleep(DOUCH_CHEF_WORK_TIME)
                logger.info(f"Dough chef #{self._id} finished pizza #{order_id}, time = {perf_counter()}")

                await self.out_queue.put(order_data)
                self.in_queue.task_done()

            except Exception:
                logger.warning(f"Encountered error:", exc_info=True)
                self.in_queue.task_done()


class ToppingChef(Worker):
    def __init__(self, *args):
        super().__init__(*args)

    async def job(self) -> None:
        while True:
            try:
                # grab an item from the queue (if there is one)
                order_data = await self.in_queue.get()

                # read the data we need to perform the work
                order_id: int = order_data.order_id
                topping: List[str] = order_data.topping

                # do some work that takes some time - simulated here with an async sleep
                logger.info(f"Topping chef #{self._id} starting pizza #{order_id}, time = {perf_counter()}")

                while topping:
                    curr_topping = topping[:2]
                    topping = topping[2:]
                    logger.info(
                        f"Topping chef #{self._id} adding {curr_topping} to pizza #{order_id}, time = {perf_counter()}")
                    await asyncio.sleep(TOPPING_CHEF_WORK_TIME)  # random wait time up to 2 seconds

                logger.info(f"Topping chef #{self._id} finished pizza #{order_id}, time = {perf_counter()}")

                await self.out_queue.put(order_data)

                self.in_queue.task_done()

            except Exception:
                logger.warning(f"Encountered error:", exc_info=True)
                self.in_queue.task_done()


class Oven(Worker):
    def __init__(self, *args):
        super().__init__(*args)

    async def job(self) -> None:
        while True:
            try:
                # grab an item from the queue (if there is one)
                order_data = await self.in_queue.get()

                # read the data we need to perform the work
                order_id: int = order_data.order_id

                # do some work that takes some time - simulated here with an async sleep
                logger.info(f"Oven #{self._id} start baking pizza #{order_id}, time = {perf_counter()}")
                await asyncio.sleep(OVEN_WORK_TIME)  # random wait time up to 2 seconds
                logger.info(f"Oven #{self._id} finished baking pizza #{order_id}, time = {perf_counter()}")

                await self.out_queue.put(order_data)
                self.in_queue.task_done()

            except Exception:
                logger.warning(f"Encountered error:", exc_info=True)
                self.in_queue.task_done()


class Waiter(Worker):
    def __init__(self, *args):
        super().__init__(*args)

    async def job(self) -> None:
        while True:
            try:
                # grab an item from the queue (if there is one)
                order_data = await self.in_queue.get()
                order_id = order_data.order_id

                # do some work that takes some time - simulated here with an async sleep
                logger.info(f"Waiter #{self._id} start serving pizza #{order_id}, time = {perf_counter()}")
                await asyncio.sleep(WAITER_WORK_TIME)  # random wait time up to 2 seconds
                end = perf_counter()
                logger.info(f"Waiter #{self._id} finished serving pizza #{order_id}, time = {end}")
                order_data.end_time = end

                self.in_queue.task_done()

            except Exception:
                logger.warning(f"Encountered error:", exc_info=True)
                self.in_queue.task_done()
