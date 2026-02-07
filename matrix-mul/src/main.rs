use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let addr = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "localhost:50051".to_string());
    let mode = args.get(2).cloned().unwrap_or_else(|| "client".to_string());

    let mut mm = matrix_mul::MatrixMul::connect(addr.clone()).await?;

    let m: usize = args.get(3).unwrap_or(&"2".to_string()).parse()?;
    let n: usize = args.get(4).unwrap_or(&"2".to_string()).parse()?;
    let p: usize = args.get(5).unwrap_or(&"2".to_string()).parse()?;

    mm.set_size(m, n, p);

    match mode.as_str() {
        "load" => {
            let m: usize = args.get(3).unwrap_or(&"2".to_string()).parse()?;
            let n: usize = args.get(4).unwrap_or(&"2".to_string()).parse()?;
            let p: usize = args.get(5).unwrap_or(&"2".to_string()).parse()?;

            let mut a = vec![vec![0.0; n]; m];
            let mut b = vec![vec![0.0; p]; n];

            let mut val_a = 1.0;
            for row in &mut a {
                for elem in row {
                    *elem = val_a;
                    val_a += 1.0;
                }
            }

            let mut val_b = 1.0;
            for row in &mut b {
                for elem in row {
                    *elem = val_b;
                    val_b += 1.0;
                }
            }

            let mut mm = mm;
            println!("Loading {}x{} matrix A and {}x{} matrix B", m, n, n, p);
            println!("Matrix A:");
            for row in &a {
                println!("  {:?}", row);
            }
            println!("Matrix B:");
            for row in &b {
                println!("  {:?}", row);
            }
            mm.load_matrices(a, b).await?;
            println!("Matrices loaded. Run 'start' to begin computation.");
        }
        "start" => {
            println!("Starting computation...");
            mm.start().await?;
            println!("Computation started. Workers can now run.");
        }
        "client" => {
            let worker_id = std::process::id();
            println!("Starting worker (PID: {})...", worker_id);
            println!("Connecting to {}...", addr);
            mm.work().await?;
            println!("Worker (PID: {}) done!", worker_id);
        }
        "result" => {
            let m: usize = args.get(3).unwrap_or(&"2".to_string()).parse()?;
            let p: usize = args.get(4).unwrap_or(&"2".to_string()).parse()?;

            println!("Waiting for completion...");
            mm.wait_for_completion(m, p).await?;
            println!("Retrieving result...");
            let result = mm.get_result(m, p).await?;

            println!("Result ({}x{}):", m, p);
            for row in result {
                println!("  {:?}", row);
            }
        }
        _ => {
            eprintln!("Unknown mode: {}", mode);
            eprintln!("Usage: {} <addr> <mode> [args...]", args[0]);
            eprintln!("Modes:");
            eprintln!("  load <m> <n> <p>  - Load m×n and n×p matrices");
            eprintln!("  start              - Start computation");
            eprintln!("  client             - Run worker (default)");
            eprintln!("  result <m> <p>     - Get result matrix");
            std::process::exit(1);
        }
    }

    Ok(())
}
