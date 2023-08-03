package com.jatin.ewallet.wallet.controller;

import com.jatin.ewallet.wallet.domain.Wallet;
import com.jatin.ewallet.wallet.service.WalletService;
import com.jatin.ewallet.wallet.service.resource.WalletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "wallet")
public class WalletController {

    @Autowired
    WalletService walletService;

    @GetMapping(value = "/{userId}")
    public ResponseEntity<?> getUserWallet(@PathVariable String userId){
        Wallet wallet=walletService.getUserWallet(userId);
        return new ResponseEntity<>(wallet.toResponse(), HttpStatus.OK);
    }
}
