import multiprocessing
import logging

logging.basicConfig(
    level=logging.INFO,  # Set the desired log level
    format='%(asctime)s [%(levelname)s] [Process-%(process)s] %(message)s',  # Define log format with process ID
    filename='consume_log.log'  # Specify the log file
)

def worker_function(number):
    # Perform intensive computation or heavy work here
    result = number * 2
    
    # Log data using the logging module
    print("oi !")
    logging.info(f"Processed {number} - Result: {result}")


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=4)

    # Execute worker_function in parallel without waiting for results
    for i in range(4):
        pool.apply_async(worker_function, args=(i,))

    # Close the pool and wait for all tasks to complete
    pool.close()
    pool.join()