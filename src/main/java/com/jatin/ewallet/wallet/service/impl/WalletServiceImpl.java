package com.jatin.ewallet.wallet.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jatin.ewallet.wallet.domain.Wallet;
import com.jatin.ewallet.wallet.repository.WalletRepository;
import com.jatin.ewallet.wallet.service.WalletService;
import com.jatin.ewallet.wallet.service.resource.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Objects;
import java.util.Random;

@Service
public class WalletServiceImpl implements WalletService {

    @Autowired
    WalletRepository walletRepository;

    @Autowired
    KafkaTemplate kafkaTemplate;

    private final String wallet_update_topic="WALLET_UPDATE";

    private ObjectMapper mapper=new ObjectMapper();

    Logger logger= LoggerFactory.getLogger(WalletServiceImpl.class);

    @Override
    public Wallet getUserWallet(String userId) {
        return walletRepository.findByUserId(Long.valueOf(userId));
    }

    @Override
    public Wallet createNewWallet(String userId) {
        Wallet wallet=new Wallet();
        wallet.setUserId(Long.valueOf(userId));
        wallet.setBalance(0.0);
        wallet.setActive(true);
        return walletRepository.save(wallet);
    }

    @Override
    public Wallet disableActiveWallet(String userId) {
        Wallet wallet=walletRepository.findByUserId(Long.valueOf(userId));
        if(Objects.nonNull(wallet)){
            wallet.setActive(false);
            //if balance not zero, trigger bank transaction
            return walletRepository.save(wallet);
        }
        return null;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW,rollbackFor = NullPointerException.class , noRollbackFor = ArithmeticException.class)
    public void updateWallet(Transaction transaction) throws JsonProcessingException {
        try {


            if (Objects.nonNull(transaction)
                    && transaction.getSenderId().equals(-99L)) {

                Wallet receiverWallet = walletRepository.findByUserId(transaction.getReceiverId());
                if (Objects.isNull(receiverWallet)) {
                    logger.error("Invalid receiver Id");
                }
                updateUserWallet(receiverWallet, transaction.getAmount());

            } else if (Objects.nonNull(transaction)
                    && transaction.getReceiverId().equals(-99L)) {
                Wallet senderWallet = walletRepository.findByUserId(transaction.getSenderId());
                if (Objects.isNull(senderWallet)) {
                    logger.error("Invalid Sender Id");
                }
                updateUserWallet(senderWallet, -1 * transaction.getAmount());
            } else if (Objects.nonNull(transaction)) {
                Wallet receiverWallet = walletRepository.findByUserId(transaction.getReceiverId());
                if (Objects.isNull(receiverWallet)) {
                    logger.error("Invalid receiver Id");
                }
                Wallet senderWallet = walletRepository.findByUserId(transaction.getSenderId());
                if (Objects.isNull(senderWallet)) {
                    logger.error("Invalid Sender Id");
                }
                performTransaction(senderWallet, receiverWallet, transaction.getAmount());
            } else {
                logger.error("Invalid Transaction Status");
            }
            transaction.setStatus("SUCCESS");
        }catch (Exception ex){
            transaction.setStatus("FAILURE");
        }finally {
            kafkaTemplate.send(wallet_update_topic,mapper.writeValueAsString(transaction));
        }
    }

    //@Transactional(propagation = Propagation.REQUIRED,rollbackFor = NullPointerException.class , noRollbackFor = ArithmeticException.class)
    private void performTransaction(Wallet senderWallet,Wallet receiverWallet,Double amount) {
        try {

            Wallet senderWalletCopy = new Wallet();
            Wallet receiverWalletCopy = new Wallet();
            BeanUtils.copyProperties(receiverWallet, receiverWalletCopy);
            BeanUtils.copyProperties(senderWallet, senderWalletCopy);

            logger.info("starting transaction between sender {} and receiver {}", senderWallet.getUserId(), receiverWallet.getUserId());

            senderWalletCopy.setBalance(senderWallet.getBalance() - amount);

            receiverWalletCopy.setBalance(receiverWalletCopy.getBalance() + amount);

            walletRepository.save(senderWalletCopy);
            walletRepository.save(receiverWalletCopy);
        }catch (Exception ex){
            logger.error("exception while updating balance");
            // walletRepository.save(receiverWallet);
            // walletRepository.save(senderWallet);
            throw ex;
        }

    }

    private void updateUserWallet(Wallet receiverWallet, Double amount) {
        try {
            Wallet receiverWalletCopy = new Wallet();
            BeanUtils.copyProperties(receiverWallet, receiverWalletCopy);
            receiverWalletCopy.setBalance(receiverWalletCopy.getBalance() + amount);
            walletRepository.save(receiverWalletCopy);
        }catch (Exception ex){
            logger.error("exception while updating balance");
            walletRepository.save(receiverWallet);
        }
    }


}