package com.bnctDB.bnctDB.model

import javax.persistence.Column
import javax.persistence.Entity
import javax.persistence.Id
import javax.persistence.Table

@Entity
@Table(name = "files")
class FileProcessed(
    @Id
    @Column(name = "Название файла")
    var fileName: String? = null,
)