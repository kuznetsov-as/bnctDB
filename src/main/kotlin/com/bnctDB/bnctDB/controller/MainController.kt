package com.bnctDB.bnctDB.controller

import com.bnctDB.bnctDB.model.BnctData
import com.bnctDB.bnctDB.model.FileProcessed
import com.bnctDB.bnctDB.repository.BnctRepository
import com.bnctDB.bnctDB.repository.filesRepository
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.text.NumberFormat
import java.util.*


@Controller
@RequestMapping("bnct")
class MainController(val bnctRepository: BnctRepository, val filesRepository: filesRepository) {

    fun magic(path: String) {
        val reader = BufferedReader(InputStreamReader(FileInputStream(path), "Windows-1251"))

        val csvParser = CSVParser(
            reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withDelimiter('	')
        )

//        if (!csvParser.headerMap.keys.contains("H-/Конус (мА)")) {
//
//        }

        for (csvRecord in csvParser) {

            var a: String
            var b: Double
            var c: Double
            var d: Double


            try {

                val format = NumberFormat.getInstance(Locale.FRANCE)

                a = csvRecord.get("Полная дата")
                b = format.parse(csvRecord.get("H-/Конус (мА)")).toDouble()
                c = format.parse(csvRecord.get("H-/Линза Воблова/Слив (°C)")).toDouble()
                d = format.parse(csvRecord.get("H-/Источник (Па)")).toDouble()

//                a = csvRecord.get("Полная дата")
//                b = csvRecord.get("H-/Конус (мА)").toDouble()
//                c = csvRecord.get("H-/Линза Воблова/Слив (°C)").toDouble()

                val data = BnctData(a, b, c, d)


                bnctRepository.save(data)
            } catch (e: Exception) {
               println(e.message)
            }
        }
    }

    @GetMapping("/start")
    fun start(): ResponseEntity<String> {

        val files = TreeMap<String, String>()
        val pathName = "D:\\BNCT_DATA"
        //val pathName = "\\\\bnct-monik1\\.sessions"

        File(pathName).walk().forEach {
            if (it.isFile) {
                if (filesRepository.findById(it.name).isEmpty) {
                    files[it.name] = it.path
                }
            }
        }

        for ((name, path) in files) {

            magic(path)
            filesRepository.save(FileProcessed(name))
            println("Закончили работу в файле $name")
        }

        println("Закончили работу в контроллере")
        return ResponseEntity.ok("working")
    }

}