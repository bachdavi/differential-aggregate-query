extern crate differential_aggregate_query;
extern crate differential_dataflow;
extern crate timely;

use differential_dataflow::input::InputSession;

use differential_aggregate_query::factors::GenericFactor;

use differential_aggregate_query::semiring::sum_prod::SumProd;
use differential_aggregate_query::{Factor, InsideOut, Query, Value};

fn main() {
    timely::execute_from_args(std::env::args(), move |worker| {
        // Create an input session per factor
        let mut i_e = InputSession::new();
        let mut i_b = InputSession::new();
        let mut i_ea = InputSession::new();
        let mut i_ba = InputSession::new();
        let mut i_ja = InputSession::new();
        let mut i_ma = InputSession::new();

        // Burglar Example
        worker.dataflow::<u64, _, _>(|scope| {
            let psi_e = GenericFactor::new(vec![1], i_e.to_collection(scope));
            let psi_b = GenericFactor::new(vec![2], i_b.to_collection(scope));
            let psi_ea = GenericFactor::new(vec![1, 3], i_ea.to_collection(scope));
            let psi_ba = GenericFactor::new(vec![2, 3], i_ba.to_collection(scope));
            let psi_ja = GenericFactor::new(vec![4, 3], i_ja.to_collection(scope));
            let psi_ma = GenericFactor::new(vec![5, 3], i_ma.to_collection(scope));

            // Marginal maximum aposteriori with 3 = "A" as free variable
            let faq = Query {
                factors: vec![psi_e, psi_b, psi_ea, psi_ba, psi_ja, psi_ma],
                variable_order: vec![1, 2, 4, 5],
            };

            // Run insideOut on out FAQ query
            let output = faq.inside_out();

            //Examin the output
            output.tuples().inspect(|x| println!("{:?}", x));
        });

        // Input the probabilities
        i_e.advance_to(0);
        i_b.advance_to(0);
        i_ea.advance_to(0);
        i_ba.advance_to(0);
        i_ja.advance_to(0);
        i_ma.advance_to(0);
        i_e.update(
            vec![Value::String("E".to_string())],
            SumProd { value: 0.01 },
        );
        i_e.update(
            vec![Value::String("!E".to_string())],
            SumProd { value: 0.99 },
        );
        i_b.update(
            vec![Value::String("B".to_string())],
            SumProd { value: 0.05 },
        );
        i_b.update(
            vec![Value::String("!B".to_string())],
            SumProd { value: 0.95 },
        );
        i_ea.update(
            vec![
                Value::String("E".to_string()),
                Value::String("A".to_string()),
            ],
            SumProd { value: 0.9 },
        );
        i_ea.update(
            vec![
                Value::String("E".to_string()),
                Value::String("!A".to_string()),
            ],
            SumProd { value: 0.1 },
        );
        i_ba.update(
            vec![
                Value::String("B".to_string()),
                Value::String("A".to_string()),
            ],
            SumProd { value: 0.6 },
        );
        i_ba.update(
            vec![
                Value::String("B".to_string()),
                Value::String("!A".to_string()),
            ],
            SumProd { value: 0.4 },
        );
        i_ja.update(
            vec![
                Value::String("J".to_string()),
                Value::String("A".to_string()),
            ],
            SumProd { value: 0.6 },
        );
        i_ja.update(
            vec![
                Value::String("J".to_string()),
                Value::String("!A".to_string()),
            ],
            SumProd { value: 0.4 },
        );
        i_ma.update(
            vec![
                Value::String("M".to_string()),
                Value::String("A".to_string()),
            ],
            SumProd { value: 0.8 },
        );
        i_ma.update(
            vec![
                Value::String("M".to_string()),
                Value::String("!A".to_string()),
            ],
            SumProd { value: 0.2 },
        );
    })
    .expect("Computation terminated abnormally");
}
