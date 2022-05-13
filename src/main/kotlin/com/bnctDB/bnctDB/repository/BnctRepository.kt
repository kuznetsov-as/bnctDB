package com.bnctDB.bnctDB.repository

import com.bnctDB.bnctDB.model.BnctData
import org.springframework.data.jpa.repository.JpaRepository
import java.time.LocalDateTime

interface BnctRepository : JpaRepository<BnctData?, LocalDateTime?>