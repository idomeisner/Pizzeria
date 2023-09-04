import json
import os
from random import sample, randint
from utils import CURR_DIR

MIN_ORDERS = 5
MAX_ORDERS = 20
MIN_TOPPING = 0
MAX_TOPPING = 8
TOPPING = [
    "Anchovy",
    "Black olives",
    "Extra cheese",
    "Fresh basil",
    "Fresh garlic",
    "Green olives",
    "Green pepper",
    "Mushroom",
    "Onion",
    "Pepperoni",
    "Tomato",
]


def generate_order() -> None:
    """
    Generates a random pizza order file

    :return:
    """

    res = []
    num_of_orders = randint(MIN_ORDERS, MAX_ORDERS)

    for _ in range(num_of_orders):
        order = sample(TOPPING, randint(MIN_TOPPING, MAX_TOPPING))
        res.append({"Topping": order})

    final_order = {"Pizzas": res}

    orders_file = os.path.join(CURR_DIR, "pizza_orders.json")
    with open(orders_file, "w") as f:
        json.dump(final_order, fp=f, indent=4)
