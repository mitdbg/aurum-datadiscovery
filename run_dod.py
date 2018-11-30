import argparse
from DoD import dod

if __name__ == "__main__":
    print("Run DoD")

    # Argument parsing
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_path', help='Path to Aurum model')
    parser.add_argument('--separator', default=',', help='CSV separator')
    parser.add_argument('--output_path', default=False, help='Path to store output views')
    parser.add_argument('--interactive', default=True, help='Run DoD in interactive mode or not')
    parser.add_argument('--full_view', default=False, help='Whether to output raw view or not')
    parser.add_argument('--list_attributes', help='Schema of View')
    parser.add_argument('--list_values', help='Values of View, leave "" if no value for a given attr is given')

    args = parser.parse_args()

    dod.main(args)

