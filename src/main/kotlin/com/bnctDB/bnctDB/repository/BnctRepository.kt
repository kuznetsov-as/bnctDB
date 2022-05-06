package com.bnctDB.bnctDB.repository

import com.bnctDB.bnctDB.model.BnctData
import org.springframework.data.jpa.repository.JpaRepository

interface BnctRepository : JpaRepository<BnctData?, String?>