#!/bin/bash

python review_per_listing.py \
    --listings ../data-src/listings.csv.gz \
    --reviews ../data-src/reviews.csv.gz \
    --outputs ./outputs/reviews_per_listing
