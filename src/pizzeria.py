import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type

from utils import CURR_DIR, config, get_logger, get_time
from workers import DouchChef, Oven, ToppingChef, Waiter, Worker

logger = get_logger()


@dataclass
class Order:
    order_id: int
    topping: List[str]
    start_time: Optional[float] = None
    end_time: Optional[float] = None


class Pizzeria:
    dough_queue: asyncio.Queue
    topping_queue: asyncio.Queue
    oven_queue: asyncio.Queue
    waiter_queue: asyncio.Queue

    def __init__(self):
        self.douch_chefs = config["DOUCH_CHEFS"]
        self.topping_chefs = config["TOPPING_CHEFS"]
        self.ovens = config["OVENS"]
        self.waiters = config["WAITERS"]
        self.orders: List[Order] = []
        self.tasks = []

    async def run(self):
        """
        The pizzeria runner function

        :return:
        """

        self.dough_queue = asyncio.Queue()
        self.topping_queue = asyncio.Queue()
        self.oven_queue = asyncio.Queue()
        self.waiter_queue = asyncio.Queue()

        self.generate_initial_queue()

        self.generate_tasks(
            self.douch_chefs, DouchChef, self.dough_queue, self.topping_queue
        )
        self.generate_tasks(
            self.topping_chefs, ToppingChef, self.topping_queue, self.oven_queue
        )
        self.generate_tasks(self.ovens, Oven, self.oven_queue, self.waiter_queue)
        self.generate_tasks(self.waiters, Waiter, self.waiter_queue)

        await self.dough_queue.join()
        await self.topping_queue.join()
        await self.oven_queue.join()
        await self.waiter_queue.join()

        for task in self.tasks:
            task.cancel()

        self.generate_report(get_time())

    def generate_initial_queue(self) -> None:
        """
        Fills the initial queue with orders, which will later be processed

        :return:
        """
        orders_file = os.path.join(CURR_DIR, "pizza_orders.json")
        with open(orders_file, "r") as f:
            task_data = json.load(f)

        for count, order_data in enumerate(task_data["Pizzas"]):
            order = Order(count, order_data["Topping"])
            self.orders.append(order)
            self.dough_queue.put_nowait(order)

    def generate_tasks(
        self,
        workers_count: int,
        base_worker: Type[Worker],
        in_queue: asyncio.Queue,
        out_queue: Optional[asyncio.Queue] = None,
    ) -> None:
        """
        Creates the asyncio task of the workers

        :param workers_count: number of workers to be created
        :param base_worker: the worker class sent to the function
        :param in_queue: the input queue from which the worker take its tasks
        :param out_queue: the output queue to which the worker puts its finished task
        :return:
        """
        for i in range(workers_count):
            worker = base_worker(i, in_queue, out_queue)
            self.tasks.append(asyncio.create_task(worker.job()))

    def generate_report(self, total_time: float) -> None:
        """
        Generate the orders report and prints summary.

        :param total_time: the total time all orders took, from first order to last order
        :return:
        """
        report: Dict[str, Any] = dict()
        orders_res: Dict[str, Dict[str, float]] = {}
        all_orders_time: float = 0

        logger.info("\nREPORT:")
        logger.info(f"Total time: {round(total_time)}sec")
        logger.info("Preparation time of each order:")

        for order in self.orders:
            order_time = order.end_time - order.start_time
            order_key = f"Order {order.order_id + 1}"
            orders_res[order_key] = {
                "Start Time": order.start_time,
                "End Time": order.end_time,
                "Total Time": order_time,
            }

            all_orders_time += order_time
            logger.info(f"Order {order.order_id + 1}: {round(order_time)}sec")

        # calculates the average order time
        average_time = (
            round(all_orders_time / len(self.orders)) if len(self.orders) else 0
        )
        logger.info(f"Average order time: {average_time}sec")

        report["Orders"] = orders_res
        report["TotalTime"] = round(total_time)
        report["AverageTime"] = average_time

        report_file = os.path.join(CURR_DIR, "report.json")
        with open(report_file, "w") as f:
            json.dump(report, fp=f, indent=4)


def run() -> None:
    try:
        asyncio.run(Pizzeria().run())

    except Exception:
        logger.warning("Encountered error:", exc_info=True)
