package com.bnctDB.bnctDB.repository

import com.bnctDB.bnctDB.model.FileProcessed
import org.springframework.data.jpa.repository.JpaRepository

interface filesRepository : JpaRepository<FileProcessed?, String?>