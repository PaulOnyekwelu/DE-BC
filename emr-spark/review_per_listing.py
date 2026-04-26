#!/usr/bin/env python
# coding: utf-8

import argparse
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, round, col, regexp_replace

parser = argparse.ArgumentParser()

parser.add_argument("--listings", required=True)
parser.add_argument("--reviews", required=True)
parser.add_argument("--outputs", required=True)

args = parser.parse_args()
listing_data = args.listings
reviews_data = args.reviews
output = args.outputs

# listing_data = "../data-src/listings.csv.gz"
# reviews_data = "../data-src/reviews.csv.gz"
spark = SparkSession.builder.appName("review-per-listing").getOrCreate()

listings = spark.read.csv(
    listing_data,
    header=True,
    inferSchema=True,
    multiLine=True,
    sep=",",
    quote='"',
    escape='"',
    mode="PERMISSIVE",
)
reviews = spark.read.csv(
    reviews_data,
    header=True,
    inferSchema=True,
    multiLine=True,
    sep=",",
    escape='"',
    quote='"',
    mode="PERMISSIVE",
)

listings = listings.select("id", "name", "price", "source")
listings = listings.withColumnRenamed("id", "listing_id")

reviews_per_listing = listings.join(reviews, on="listing_id", how="left").withColumn(
    "price", regexp_replace("price", "[$,]", "").cast("double")
)
reviews_per_listing_export = reviews_per_listing.groupBy(
    ["listing_id", "name", "source"]
).agg(
    round(avg("price"), 2).alias("price"),
    count("*").alias("num_reviews"),
)

# reviews_per_listing_export.show(5)

reviews_per_listing_export.write.format("csv").option("header", True).mode("overwrite").save(output)
