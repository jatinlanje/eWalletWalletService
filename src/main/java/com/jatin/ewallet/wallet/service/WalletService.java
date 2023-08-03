package com.jatin.ewallet.wallet.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.jatin.ewallet.wallet.domain.Wallet;
import com.jatin.ewallet.wallet.service.resource.Transaction;

public interface WalletService {

    Wallet getUserWallet(String userId);

    Wallet createNewWallet(String userId);

    Wallet disableActiveWallet(String userId);

    void updateWallet(Transaction transaction) throws JsonProcessingException;
}
