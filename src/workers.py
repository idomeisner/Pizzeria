import asyncio
from abc import ABC, abstractmethod
from typing import List, Optional
from utils import config, get_logger, get_time

logger = get_logger()


class Worker(ABC):
    """
    Base class of each of the workers types
    """

    work_duration: int

    def __init__(
        self,
        idx: int,
        in_queue: asyncio.Queue,
        out_queue: Optional[asyncio.Queue] = None,
    ):
        self._id = idx
        self.in_queue = in_queue
        self.out_queue = out_queue

    @abstractmethod
    async def job(self) -> None:
        pass


class DouchChef(Worker):
    def __init__(self, *args):
        super().__init__(*args)
        self.work_duration = config["DOUCH_CHEF_WORK_DURATION"]

    async def job(self) -> None:
        while True:
            try:
                # takes his next task (order) from the queue
                order_data = await self.in_queue.get()
                order_id = order_data.order_id

                start = get_time()
                order_data.start_time = start
                logger.info(
                    f"Dough chef #{self._id} starting pizza #{order_id}, time = {start}"
                )
                await asyncio.sleep(self.work_duration)  # working on the douch
                logger.info(
                    f"Dough chef #{self._id} finished pizza #{order_id}, time = {get_time()}"
                )

                # puts the order in the queue for the next worker in the pipeline
                await self.out_queue.put(order_data)
                self.in_queue.task_done()

            except Exception:
                logger.warning(f"Encountered error:", exc_info=True)
                self.in_queue.task_done()


class ToppingChef(Worker):
    def __init__(self, *args):
        super().__init__(*args)
        self.work_duration = config["TOPPING_CHEF_WORK_DURATION"]

    async def job(self) -> None:
        while True:
            try:
                # takes his next task (order) from the queue
                order_data = await self.in_queue.get()
                order_id: int = order_data.order_id
                topping: List[str] = order_data.topping

                logger.info(
                    f"Topping chef #{self._id} starting pizza #{order_id}, time = {get_time()}"
                )

                while topping:
                    curr_topping = topping[:2]
                    topping = topping[2:]
                    logger.info(
                        f"Topping chef #{self._id} adding {curr_topping} to pizza #{order_id}, time = {get_time()}"
                    )
                    await asyncio.sleep(self.work_duration)  # working on the topping

                logger.info(
                    f"Topping chef #{self._id} finished pizza #{order_id}, time = {get_time()}"
                )

                # puts the order in the queue for the next worker in the pipeline
                await self.out_queue.put(order_data)
                self.in_queue.task_done()

            except Exception:
                logger.warning(f"Encountered error:", exc_info=True)
                self.in_queue.task_done()


class Oven(Worker):
    def __init__(self, *args):
        super().__init__(*args)
        self.work_duration = config["OVEN_WORK_DURATION"]

    async def job(self) -> None:
        while True:
            try:
                # takes his next task (order) from the queue
                order_data = await self.in_queue.get()
                order_id: int = order_data.order_id

                logger.info(
                    f"Oven #{self._id} start baking pizza #{order_id}, time = {get_time()}"
                )
                await asyncio.sleep(self.work_duration)  # baking the pizza
                logger.info(
                    f"Oven #{self._id} finished baking pizza #{order_id}, time = {get_time()}"
                )

                # puts the order in the queue for the next worker in the pipeline
                await self.out_queue.put(order_data)
                self.in_queue.task_done()

            except Exception:
                logger.warning(f"Encountered error:", exc_info=True)
                self.in_queue.task_done()


class Waiter(Worker):
    def __init__(self, *args):
        super().__init__(*args)
        self.work_duration = config["WAITER_WORK_DURATION"]

    async def job(self) -> None:
        while True:
            try:
                # takes his next task (order) from the queue
                order_data = await self.in_queue.get()
                order_id = order_data.order_id

                logger.info(
                    f"Waiter #{self._id} starts serving pizza #{order_id}, time = {get_time()}"
                )
                await asyncio.sleep(self.work_duration)  # serving the pizza
                end = get_time()
                logger.info(
                    f"Waiter #{self._id} finished serving pizza #{order_id}, time = {end}"
                )
                order_data.end_time = end

                self.in_queue.task_done()

            except Exception:
                logger.warning(f"Encountered error:", exc_info=True)
                self.in_queue.task_done()
