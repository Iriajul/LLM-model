from workflow import app, WorkflowState
from config import logger

if __name__ == "__main__":
    logger.info("Starting NL2SQL application example...")

    # --- Example Usage ---
    # Replace this with the question you want to ask
    # Example questions based on the original project's test cases:
    #question = "List all customers name and email address"
    #question = "List sales representatives and the number of orders they handled in their first year"
    #question = "Which product category has the highest return on stock investment?" # Might be hard without specific ROI definition
    #question = "Identify the products that show a consistent month-over-month revenue growth in the last 2 year"
    #question = "Categorize each supplier as Top, Average, or Low performer based on total revenue from their products."
    #question = "Show the cumulative revenue by month for each product category. "
    #question = "Show all orders where a discount was applied (discount_percentage > 0), including order id, customer name, and discount amount."
    #question = "Classify each order as small, medium or large based on the total amount"
    #question = "List each customer’s most recent order along with their previous order date"
    #question = "Show each revenue trend over time for each supplier, grouped by quarter"
    #question = "Find the average commission earned by each sales representative per region based on actual sales."
    question = "For each product, show the average discount applied and the effective price after discount, sorted by discount descending. "
    #question = "For each sales representative, calculate their total sales revenue and compare it to their annual target."
    #question = " Which customers placed more than 3 orders in the last 2 years and have an average order value greater than $500?"
    #question = "Determine the average delivery delay per shipping method order date vs delivery date and rank them by efficiency."
    #question = "Show me the suppliers tables data"
    #question = "Show me the products tables data"
    #question= "Show me the customers name who work for this company with 5 years"
    logger.info(f"User Question: {question}")

    # Prepare the initial state for the graph
    initial_state: WorkflowState = {
        "user_input": question,
        "messages": [],
        "db_schema": "",
        "generated_sql": "",
        "db_result": None,
        "error_message": None,
        "correction_attempts": 0
    }

    try:
        # Invoke the LangGraph application
        final_state = app.invoke(initial_state)

        if final_state and final_state.get("messages"):
            final_message = final_state["messages"][-1]
            if hasattr(final_message, 'content'):
                print("\n--- Final Answer ---")
                print(final_message.content)
                logger.info(f"Final Answer: {final_message.content}")
            else:
                logger.error("Final state message has no content.")
                print("\n--- Final State (No Content) ---")
                print(final_state)
        else:
            logger.error("Workflow finished without a final state or messages.")
            print("\n--- Workflow Error ---")
            print("The workflow did not produce a final result.")

    except Exception as e:
        logger.error(f"An error occurred during workflow execution: {e}", exc_info=True)
        print(f"\n--- Workflow Execution Error ---")
        print(f"An unexpected error occurred: {e}")

    logger.info("NL2SQL application example finished.")