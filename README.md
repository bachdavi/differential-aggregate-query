# Differential Aggregate Query

An incremental implementation of the Functional Aggregate Query (FAQ) framework in Rust using [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow).

This is a research project and currently does only provide a naive implementation of the `insideOut` algorithm to compute FAQ Queries.

Currently not concerned with optimal variable order. 

## Research
The FAQ framework is discussed in this paper [FAQ: Questions Asked Frequently](https://arxiv.org/abs/1504.04044)

## Implementation

FAQ generalizes a bunch of different, on first glance unconnected problems by expressing them as aggregates over hypergraphs.

Factors are Differential Dataflow collections with generic datatype representing the input and output in listing format. 
That means every tuple in the collection has a support and a factor value. The factor value resides in the diff.

Currently we only support the SumProd semi-ring as differential dataflows join represents the product and consolidate the sum. 

More semi-ring aggregates will be added by implementing the correct diff trait.

## Running

`cargo run --example graph` will kick off a very small triangle counting computation expressed as a FAQ.

