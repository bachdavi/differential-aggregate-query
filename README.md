# Differential Aggregate Query

An incremental implementation of the Functional Aggregate Query (FAQ) framework in Rust using [Differential Dataflow](https://github.com/comnik/declarative-dataflow).

This is a research project and currently does only provide a naive implementation of the `insideOut` algorithm to compute FAQ Queries.

Currently not concerned with optimal variable order. 

## Research
The FAQ framework is discussed in this paper [FAQ: Questions Asked Frequently](https://arxiv.org/abs/1504.04044)

## Implementation

FAQ generalizes a bunch of different, on first glance unconnected problems by expressing them as aggregates over hypergraphs.

Factors are Differential Dataflow collections with generic datatype.

In order to provide a general implementation of `insideOut` every class of factors implements their own handling of aggregates and their corresponding datatype.

Currently there is only a graph factor implementation.

## Running

`cargo run --example graph` will kick off a very small triangle counting computation expressed as a FAQ.

