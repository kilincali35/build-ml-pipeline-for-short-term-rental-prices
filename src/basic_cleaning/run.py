#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning,
exporting the result to a new artifact

User:ali.kilinc
Date:5.12.2022
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):
    """
    This function is used to download data from W&B, apply basic data cleaning,
    and logging the steps

    argument:
        args : Defining input and output artifacts, and some basic parameters
    return:
        None
    """

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Downloading artifact")
    local_path = wandb.use_artifact(args.input_artifact).file()
    
    #read the artifact and turn into a proper DF
    df = pd.read_csv(local_path, header = 0, index_col = False, sep=',')
    df_last = pd.DataFrame(df)

    #Drop the outliers related to price variable
    logger.info("Dropping outlier")
    min_price = args.min_price
    max_price = args.max_price
    idx = df_last['price'].between(min_price, max_price)
    df_last = df_last[idx].copy()

    # Convert last_review to datetime
    logger.info("Convert last_review column type to date")
    df_last['last_review'] = pd.to_datetime(df_last['last_review'])

    # Saving the artifact
    logger.info("Saving the artifact")
    df_last.to_csv("clean_sample.csv", index=False)

    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description)

    logger.info("Logging artifact instance")
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Name of the input_artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name of the output_artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description for the artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price value allowed",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price value allowed",
        required=True
    )


    args = parser.parse_args()

    go(args)
