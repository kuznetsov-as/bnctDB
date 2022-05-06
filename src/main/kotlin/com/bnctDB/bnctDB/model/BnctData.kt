package com.bnctDB.bnctDB.model

import javax.persistence.*

@Entity
@Table(name = "bnct")
class BnctData(
    @Id
    @Column(name = "Полная_Дата")
    var Polnaia_data: String? = null,
    @Column(name = "H_Конус_мА")
    var H_Konus_mA: Double? = null,
    @Column(name = "H_Линза_Воблова_Слив_C")
    var H_Linza_Voblova_Sliv_C: Double? = null,
    @Column(name = "H-/Источник (Па)")
    var H_Istochnik_Pa: Double? = null

)