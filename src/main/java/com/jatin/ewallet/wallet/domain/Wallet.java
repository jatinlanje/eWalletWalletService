package com.jatin.ewallet.wallet.domain;

import com.jatin.ewallet.wallet.service.resource.WalletResponse;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@Builder
@Entity
@NoArgsConstructor
@Table(name="wallet")
public class Wallet {

    @jakarta.persistence.Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long Id;

    private Double balance;

    private String type;

    private boolean active;

    private Long  userId;

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;


    public WalletResponse toResponse(){
        return  WalletResponse.builder().userId(userId.toString()).balance(balance.toString()).lastUpdated(updatedAt).type(type).build();
    }
}
