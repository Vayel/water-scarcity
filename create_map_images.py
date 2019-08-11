import argparse
import os
import sys

import water_scarcity as ws


if __name__ == "__main__":
    parser = argparse.ArgumentParser(epilog=(
        "Example: python create_map_images.py data/sswi.csv data/karst.json images/"
    ))
    parser.add_argument("sswi_file", type=open)
    parser.add_argument("karst_file", type=open)
    parser.add_argument(
        "dest",
        help="The path of the directory to save the json into"
    )
    try:
        args = parser.parse_args()
    except FileNotFoundError as e:
        print(e)
        sys.exit(1)

    colorscale = ["transparent", "#e08f17", "#a9381c", "#000000"]
    risks, _ = ws.compute_risks(args.sswi_file, args.karst_file, batch_size=1000)
    for horizon, horizon_data in risks.items():
        for season, points in horizon_data.items():
            print(horizon, season)
            m = ws.display(risks[horizon][season], colorscale, zoom_control=False)
            ws.map_to_png(m, os.path.join(args.dest, f"{horizon}_{season}.png"))
