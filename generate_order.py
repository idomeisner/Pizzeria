from random import sample, randint
import json


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
    "Tomato"
]


def generate_order() -> None:
    """
    Generates a random pizza order file

    :return:
    """

    res = []
    num_of_orders = randint(5, 20)

    for _ in range(num_of_orders):
        order = sample(TOPPING, randint(0, len(TOPPING)))
        res.append({"Topping": order})

    final_order = {"Pizzas": res}

    with open("pizza_orders.json", "w") as f:
        json.dump(final_order, fp=f, indent=4)
