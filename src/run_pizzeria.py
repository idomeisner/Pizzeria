import argparse
import generate_order, pizzeria


def main() -> None:
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-g", "--generate-order", dest="generate", action="store_true", help="generate new order file")
    parser.add_argument("-dr", "--dont-run", dest="dont_run", action="store_true", help="don't run pizzeria")

    args = parser.parse_args()

    if args.generate:
        generate_order.generate_order()

    if not args.dont_run:
        pizzeria.run()


if __name__ == '__main__':
    main()
