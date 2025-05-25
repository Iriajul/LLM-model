from workflow import app, WorkflowState
from config import logger

if __name__ == "__main__":
    logger.info("Starting NL2SQL application example...")

    # --- Example Usage ---
    # Replace this with the question you want to ask
    # Example questions based on the original project's test cases:
    #question = "List sales representatives and the number of orders they handled in their first year"
    # question = "Which product category has the highest return on stock investment?" # Might be hard without specific ROI definition
    #question = "Identify the products that show a consistent month-over-month revenue growth in the last 4 months"
    question = "Categorize each supplier as Top, Average, or Low performer based on total revenue from their products."
    #question = "Identify all orders that include Electronics "
    #question = "Show all orders where a discount was applied (discount_percentage > 0), including order id, customer name, and discount amount."
    #question = "Show the revenue trend over time for each supplier, grouped by quarter"
    #question = "Show the revenue trend over time for each supplier, grouped by quarter"

    logger.info(f"User Question: {question}")

    # Prepare the initial state for the graph
    # Only 'user_input' is strictly required to start
    initial_state: WorkflowState = {
        "user_input": question,
        # The rest will be populated by the graph nodes
        "messages": [],
        "db_schema": "",
        "generated_sql": "",
        "db_result": None,
        "error_message": None,
        "correction_attempts": 0
    }

    try:
        # Invoke the LangGraph application
        # The `stream` method can be used for observing intermediate steps,
        # but `invoke` is simpler for getting the final state.
        final_state = app.invoke(initial_state)

        # Extract the final message (usually the formatted answer or error)
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

