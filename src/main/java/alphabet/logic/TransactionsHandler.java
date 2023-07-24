package alphabet.logic;

import dal.TransactionRepository;
import enums.DbTransactionStatus;
import exceptions.ProcessorException;
import logger.BillingSystemLogger;
import models.db.BillingTransaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import wrappers.ProcessorWrapper;

import javax.persistence.LockModeType;
import java.util.List;
import java.util.logging.Level;

@Service
@EnableScheduling
public class TransactionsHandler {

    private final ProcessorWrapper processorWrapper;
    private final TransactionRepository transactionRepository;
    private final BillingSystemLogger logger;
    private final String srcBankAccount;

    @Autowired
    public TransactionsHandler(ProcessorWrapper processorWrapper,
                               TransactionRepository transactionRepository,
                               BillingSystemLogger logger,
                               @Value("${src.bank.account}") String srcBankAccount) {
        this.processorWrapper = processorWrapper;
        this.transactionRepository = transactionRepository;
        this.logger = logger;
        this.srcBankAccount = srcBankAccount;
    }

    @Scheduled(fixedRateString = "${handle.advance.scheduling.in.ms}")
    public void performTransactions() {
        logger.log(Level.FINE, "Performing transactions saved in the database at status " +
                "WAITING_TO_BE_SENT or FAILURE");

        try {
            tryPerformingTransactions();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Unknown exception has occurred while performing transactions: " +
                    e.getMessage());
        }
    }

    @Transactional
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    private void tryPerformingTransactions() {
        List<BillingTransaction> dbTransactionsToPerform =
                transactionRepository.findTransactionsInStatusesWaitingToBeSentOrFailure();

        if (dbTransactionsToPerform.isEmpty()) {
            logger.log(Level.FINE, "No transaction is required to be performed");
        } else {
            dbTransactionsToPerform.parallelStream().forEach(this::handleTransaction);
            logger.log(Level.INFO, "Waiting for confirmation of " + dbTransactionsToPerform.size() +
                    " attempted transactions");
        }
    }

    private void handleTransaction(BillingTransaction billingTransaction) {
        try {
            tryHandlingOneTransaction(billingTransaction);
            logger.log(Level.INFO, "Tried to perform " + billingTransaction.getTransactionDirection() +
                    " transaction: " + billingTransaction.getTransactionId() + " from " + srcBankAccount + " to "
                    + billingTransaction.getDstBankAccount() + " of amount " + billingTransaction.getAmount() +
                    ". Awaiting confirmation of successful transaction");
        } catch (ProcessorException e) {
            updateTransactionStatusInDB(billingTransaction, DbTransactionStatus.WAITING_TO_BE_SENT);
            logger.log(Level.SEVERE, "Got the following error while trying to perform transaction: "
                    + e.getMessage());
        }
    }

    private void tryHandlingOneTransaction(BillingTransaction billingTransaction) throws ProcessorException {
        updateTransactionStatusInDB(billingTransaction, DbTransactionStatus.SENT_TRANSACTION);
        String transactionId = processorWrapper.performTransaction(srcBankAccount,
                billingTransaction.getDstBankAccount(), billingTransaction.getAmount(),
                billingTransaction.getTransactionDirection());
        saveTransactionIdInDB(billingTransaction, transactionId);
    }

    private void updateTransactionStatusInDB(BillingTransaction billingTransaction,
                                             DbTransactionStatus transactionStatus) {
        billingTransaction.setTransactionStatus(transactionStatus);
        transactionRepository.save(billingTransaction);
    }

    private void saveTransactionIdInDB(BillingTransaction billingTransaction, String transactionId) {
        billingTransaction.setTransactionId(transactionId);
        transactionRepository.save(billingTransaction);
    }
}
