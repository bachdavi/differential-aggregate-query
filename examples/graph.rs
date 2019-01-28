extern crate differential_aggregate_query;
extern crate differential_dataflow;
extern crate timely;

use differential_dataflow::input::InputSession;

use differential_aggregate_query::factors::graph::{
    AggregationFn::SUM, GraphAggregate, GraphFactor,
};

use differential_aggregate_query::{Factor, InsideOut, Query};

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = InputSession::new();

        // Triangle counting
        worker.dataflow::<u64, _, _>(|scope| {
            let graph_factor_1 = GraphFactor {
                vertices: vec![2, 3],
                tuples: input.to_collection(scope),
            };
            let graph_factor_2 = GraphFactor {
                vertices: vec![1, 2],
                tuples: input.to_collection(scope),
            };
            let graph_factor_3 = GraphFactor {
                vertices: vec![1, 3],
                tuples: input.to_collection(scope),
            };

            let faq = Query {
                factors: vec![graph_factor_1, graph_factor_2, graph_factor_3],
                aggregates: vec![
                    GraphAggregate {
                        aggregation_fn: SUM,
                    },
                    GraphAggregate {
                        aggregation_fn: SUM,
                    },
                    GraphAggregate {
                        aggregation_fn: SUM,
                    },
                ],
                variable_order: vec![3, 2, 1],
            };

            // Run insideOut on out FAQ query
            let output = faq.inside_out();

            //Examin the output
            output.tuples().inspect(|x| println!("{:?}", x));
        });

        // Create a few edges
        input.advance_to(0);
        input.insert(vec![1, 2]);
        input.insert(vec![1, 3]);
        input.insert(vec![2, 3]);
        input.insert(vec![2, 4]);
        input.insert(vec![4, 5]);
        input.insert(vec![5, 6]);
        input.insert(vec![6, 7]);
        input.insert(vec![5, 7]);
    })
    .expect("Computation terminated abnormally");
}
