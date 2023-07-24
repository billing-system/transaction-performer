(1) Periodically runs to retrieve all transactions with a status of "WAITING_TO_BE_SENT" from the database.
(2) It performs each transaction using the processor.perform_transaction API, and changes the transaction status to "SENT_TRANSACTION"
(3) Receives the transaction ID from the processor.perform_transaction API call and updates the transaction's "transaction ID" in the database
 
