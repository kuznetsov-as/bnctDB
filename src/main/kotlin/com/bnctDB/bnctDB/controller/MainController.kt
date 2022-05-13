package com.bnctDB.bnctDB.controller

import com.bnctDB.bnctDB.model.BnctData
import com.bnctDB.bnctDB.model.FileProcessed
import com.bnctDB.bnctDB.repository.BnctRepository
import com.bnctDB.bnctDB.repository.filesRepository
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.text.NumberFormat
import java.text.ParseException
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.TimeUnit


@Controller
@RequestMapping("bnct")
class MainController(val bnctRepository: BnctRepository, val filesRepository: filesRepository) {

    var allHeaders = mutableListOf<MutableSet<String>>()
    var uniqueHeaders = setOf<MutableSet<String>>()
    var filesHeaders = HashMap<MutableSet<String>, MutableSet<String>>()
    var goodHeaders = mutableSetOf<String>()

    fun setAllHeadersSet(path: String) {
        val reader = BufferedReader(InputStreamReader(FileInputStream(path), "Windows-1251"))

        val csvParser = CSVParser(
            reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withDelimiter('	')
        )

        val headers = csvParser.headerMap.keys

        allHeaders.add(headers)

        reader.close()
    }

    fun findHeaders(name: String, path: String) {
        val reader = BufferedReader(InputStreamReader(FileInputStream(path), "Windows-1251"))

        val csvParser = CSVParser(
            reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withDelimiter('	')
        )

        val headers = csvParser.headerMap.keys

        for (set in filesHeaders) {
            if (set.key == headers) {
                set.value.add(name)
            }
        }

        allHeaders.add(headers)

        reader.close()
    }

    @GetMapping("/compare")
    fun compare(): ResponseEntity<String> {

        val files = TreeMap<String, String>()
        val pathName = "D:\\BNCT_DATA"

        File(pathName).walk().forEach {
            if (it.isFile) {
                files[it.name] = it.path
            }
        }

        for ((_, path) in files) {
            setAllHeadersSet(path)
        }

        println("Количество всех заголовков ${allHeaders.size} (должно быть 23)")

        uniqueHeaders = allHeaders.toSet()

        println("Количество уникальных заголовков ${uniqueHeaders.size}")

        for (set in uniqueHeaders) {
            filesHeaders[set] = mutableSetOf()
        }
        println("Размер мапы ${filesHeaders.size} (должно быть 1?)")

        for ((name, path) in files) {
            findHeaders(name, path)
        }

        for (set in filesHeaders) {
            //println("${set.value}")
            //println("${set.value.size}")

            if (set.key.toString() == "[Время, Полная дата, H-/Конус (мА), H-/Линза Воблова/Слив (°C), H-/Линза Воблова/Напор (°C), H-/Линза Воблова/Мощность (Вт), H-/Линза Воблова/Поток (л/мин), H-/Напор IFM (л/мин), H-/Слив IFM (л/мин), H-/Источник (Па), H-/Дифф. откачка (Па), Bergoz/HEBL/Ток (мА), Конус/1/Ток (мА), Конус/1/Центр (°C), Конус/1/М Верх (°C), Конус/1/М Право (°C), Конус/1/М Низ (°C), Конус/1/М Лево (°C), Конус/1/Б Верх (°C), Конус/1/Б Право (°C), Конус/1/Б Низ (°C), Конус/1/Б Лево (°C), Конус/2/Ток (мА), Конус/2/Центр (°C), Конус/2/М Верх (°C), Конус/2/М Право (°C), Конус/2/М Низ (°C), Конус/2/М Лево (°C), Конус/2/Б Верх (°C), Конус/2/Б Право (°C), Конус/2/Б Низ (°C), Конус/2/Б Лево (°C), ЭЛВ/Энергия (кэВ), ЭЛВ/Beam0 (мА), ЭЛВ/EnergyU get (кВ), ЭЛВ/EnergyU set (кВ), ЭЛВ/EnergyI (мА), ЭЛВ/Avarage (мА), ЭЛВ/Sec1U (кВ), ЭЛВ/Sec1I (мА), ЭЛВ/IsolatorU (кВ), ЭЛВ/IsolatorI (мА), ЭЛВ/Dark (мА), Охл. диаф./1/Верх (°C), Охл. диаф./1/Право (°C), Охл. диаф./1/Низ (°C), Охл. диаф./1/Лево (°C), Охл. диаф./2/Верх (°C), Охл. диаф./2/Право (°C), Охл. диаф./2/Низ (°C), Охл. диаф./2/Лево (°C), Охл. диаф./3/Верх (°C), Охл. диаф./3/Право (°C), Охл. диаф./3/Низ (°C), Охл. диаф./3/Лево (°C), Охл. диаф./4/Верх (°C), Охл. диаф./4/Право (°C), Охл. диаф./4/Низ (°C), Охл. диаф./4/Лево (°C), Охл. диаф./5/Верх (°C), Охл. диаф./5/Право (°C), Охл. диаф./5/Низ (°C), Охл. диаф./5/Лево (°C), Li мишень/Ток (мА), Li мишень/Центр (0) (°C), Li мишень/Низ-право (1) (°C), Li мишень/Право (2) (°C), Li мишень/Верх-право (3) (°C), Li мишень/Верх (4) (°C), Li мишень/Верх-лево (5) (°C), Li мишень/Лево (6) (°C), Li мишень/Низ-лево (7) (°C), Li мишень/Вода вход (л/мин), Li мишень/Вода выход 1 (л/мин), Li мишень/Вода выход 2 (л/мин), Li мишень/Вакуум (Па), Li мишень 9/Центр (0) (°C), Li мишень 9/М Верх (3) (°C), Li мишень 9/М Право (1) (°C), Li мишень 9/М Низ (7) (°C), Li мишень 9/М Лево (5) (°C), Li мишень 9/Б Верх (4) (°C), Li мишень 9/Б Право (2) (°C), Li мишень 9/Б Низ (8) (°C), Li мишень 9/Б Лево (6) (°C), Боп-1М/Б2/Зал/Гамма (Зв/Ч), Боп-1М/Б2/Зал/Нейтроны (Зв/Ч), Боп-1М/Б2/Кор/Гамма (Зв/Ч), Боп-1М/Б2/Кор/Нейтроны (Зв/Ч), Боп-1М/Б3/Зал/Гамма (Зв/Ч), Боп-1М/Б3/Зал/Нейтроны (Зв/Ч), Боп-1М/Б3/Кор/Гамма (Зв/Ч), Боп-1М/Б3/Кор/Нейтроны (Зв/Ч), Li6/Нейтроны (шт/сек), Li6/Гамма (шт/сек), Li6/Загрузка (%), Li6/Интеграл нейтронов (шт), Эфф. обдирки/Нейтралы I (мА), Эфф. обдирки/Эфф. (%), UltravoltPs/300V/U set (В), UltravoltPs/300V/I set (мА), UltravoltPs/300V/U get (В), UltravoltPs/300V/I get (мА), БДН/A/n (шт/сек), БДН/A/g (шт/сек), БДН/A/gn (шт/сек), БДН/A/gSv (Зв/Ч), БДН/A/nSv (Зв/Ч), БДН/B/n (шт/сек), БДН/B/g (шт/сек), БДН/B/gn (шт/сек), БДН/B/gSv (Зв/Ч), БДН/B/nSv (Зв/Ч), БДН/C/n (шт/сек), БДН/C/g (шт/сек), БДН/C/gn (шт/сек), БДН/C/gSv (Зв/Ч), БДН/C/nSv (Зв/Ч), Обдир миш/Слив (°C), Обдир миш/Напор (°C), Обдир миш/Мощность (Вт), Обдир миш/Поток (л/мин), Обдир миш/Аргон, HPGe/Мёртвое время (%), HPGe/Скорость в интеграле (шт/сек), HPGe/Интеграл (шт), Вычислятор флюенса (Bergoz/HEBL/Ток)/Флюенс (мАч), Вычислятор флюенса (ЭЛВ/Beam0)/Флюенс (мАч), Вычислятор флюенса (Боп-1М/Б2/Зал/Гамма)/Флюенс (мАч), Вычислятор флюенса (Боп-1М/Б2/Зал/Нейтроны)/Флюенс (мАч), Вычислятор флюенса (Li6/Нейтроны)/Флюенс (мАч), Ускоритель/Входная охлаждаемая диафрагма/Слив (°C), Ускоритель/Входная охлаждаемая диафрагма/Напор (°C), Ускоритель/Входная охлаждаемая диафрагма/Мощность (Вт), Ускоритель/Входная охлаждаемая диафрагма/Поток (л/мин), Вакуум/Ускоритель выход (Па), Вакуум/HEBL После развертки (Па), operatorPcDT, journal, ]") { //? ХАРДКОД
                goodHeaders = set.key
            }
        }


        for (set in filesHeaders) {
            val badHeaders = mutableSetOf<String>()
            val missingHeaders = mutableSetOf<String>()

            for (header in set.key) {
                if (!goodHeaders.contains(header)) {
                    badHeaders.add(header)
                }
            }

            for (header in goodHeaders) {
                if (!set.key.contains(header)) {
                    missingHeaders.add(header)
                }

            }


            if ((badHeaders.isNotEmpty()) and (missingHeaders.isNotEmpty())) {
                println("==================================")
                println("В файлах:")
                for (it in set.value) {
                    println(it)
                }

                println("----------------------------------")
                println("Найдены лишние колонки:")
                for (it in badHeaders) {
                    println(it)
                }
                println("----------------------------------")
                println("Отсутствуют обязательные заголовки:")
                for (it in missingHeaders) {
                    println(it)
                }
            }
        }

        return ResponseEntity.ok("compare is done")
    }

    fun getFileData(name: String, path: String, headers: MutableSet<String>): MutableList<BnctData> {
        val reader = BufferedReader(InputStreamReader(FileInputStream(path), "Windows-1251"))

        val csvParser = CSVParser(
            reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withDelimiter('	')
        )

        val resultData = mutableListOf<BnctData>()

        fun parseCSVtoDouble(csvRecord: CSVRecord, header: String): Double? {
            val format = NumberFormat.getInstance(Locale.FRANCE)
            return try {
                format.parse(csvRecord.get(header)).toDouble()
            } catch (e: ParseException) {
                null
            }
        }

        for (csvRecord in csvParser) {
            val dateAndTimePattern = DateTimeFormatter.ofPattern("yyyy-MM-dd@HH:mm:ss.SSS")
            val dateAndTime = LocalDateTime.parse(csvRecord.get("Полная дата"), dateAndTimePattern)

            val operatorTimePattern = DateTimeFormatter.ofPattern("yyyy-MM-dd--HH-mm-ss")
            var operatorDateAndTime: LocalDateTime? = null

            if (csvRecord.get("operatorPcDT").isNotEmpty()) {
                //val date = csvRecord.get("operatorPcDT").replace("--", "@")
                operatorDateAndTime = LocalDateTime.parse(csvRecord.get("operatorPcDT"), operatorTimePattern)
            }

            val data = BnctData(
                dateAndTime,
                parseCSVtoDouble(csvRecord, "H-/Конус (мА)"),
                parseCSVtoDouble(csvRecord, "H-/Линза Воблова/Слив (°C)"),
                parseCSVtoDouble(csvRecord, "H-/Линза Воблова/Напор (°C)"),
                parseCSVtoDouble(csvRecord, "H-/Линза Воблова/Мощность (Вт)"),
                parseCSVtoDouble(csvRecord, "H-/Линза Воблова/Поток (л/мин)"),
                parseCSVtoDouble(csvRecord, "H-/Напор IFM (л/мин)"),
                parseCSVtoDouble(csvRecord, "H-/Слив IFM (л/мин)"),
                parseCSVtoDouble(csvRecord, "H-/Источник (Па)"),
                parseCSVtoDouble(csvRecord, "H-/Дифф. откачка (Па)"),
                parseCSVtoDouble(csvRecord, "Bergoz/HEBL/Ток (мА)"),
                parseCSVtoDouble(csvRecord, "Конус/1/Ток (мА)"),
                parseCSVtoDouble(csvRecord, "Конус/1/Центр (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/1/М Верх (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/1/М Право (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/1/М Низ (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/1/М Лево (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/1/Б Верх (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/1/Б Право (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/1/Б Низ (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/1/Б Лево (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/2/Ток (мА)"),
                parseCSVtoDouble(csvRecord, "Конус/2/Центр (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/2/М Верх (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/2/М Право (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/2/М Низ (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/2/М Лево (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/2/Б Верх (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/2/Б Право (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/2/Б Низ (°C)"),
                parseCSVtoDouble(csvRecord, "Конус/2/Б Лево (°C)"),
                parseCSVtoDouble(csvRecord, "ЭЛВ/Энергия (кэВ)"),
                parseCSVtoDouble(csvRecord, "ЭЛВ/Beam0 (мА)"),
                parseCSVtoDouble(csvRecord, "ЭЛВ/EnergyU get (кВ)"),
                parseCSVtoDouble(csvRecord, "ЭЛВ/EnergyU set (кВ)"),
                parseCSVtoDouble(csvRecord, "ЭЛВ/EnergyI (мА)"),
                parseCSVtoDouble(csvRecord, "ЭЛВ/Avarage (мА)"),
                parseCSVtoDouble(csvRecord, "ЭЛВ/Sec1U (кВ)"),
                parseCSVtoDouble(csvRecord, "ЭЛВ/Sec1I (мА)"),
                parseCSVtoDouble(csvRecord, "ЭЛВ/IsolatorU (кВ)"),
                parseCSVtoDouble(csvRecord, "ЭЛВ/IsolatorI (мА)"),
                parseCSVtoDouble(csvRecord, "ЭЛВ/Dark (мА)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./1/Верх (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./1/Право (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./1/Низ (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./1/Лево (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./2/Верх (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./2/Право (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./2/Низ (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./2/Лево (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./3/Верх (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./3/Право (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./3/Низ (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./3/Лево (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./4/Верх (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./4/Право (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./4/Низ (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./4/Лево (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./5/Верх (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./5/Право (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./5/Низ (°C)"),
                parseCSVtoDouble(csvRecord, "Охл. диаф./5/Лево (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Ток (мА)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Центр (0) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Низ-право (1) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Право (2) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Верх-право (3) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Верх (4) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Верх-лево (5) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Лево (6) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Низ-лево (7) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Вода вход (л/мин)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Вода выход 1 (л/мин)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Вода выход 2 (л/мин)"),
                parseCSVtoDouble(csvRecord, "Li мишень/Вакуум (Па)"),
                parseCSVtoDouble(csvRecord, "Li мишень 9/Центр (0) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень 9/М Право (1) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень 9/Б Право (2) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень 9/М Верх (3) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень 9/Б Верх (4) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень 9/М Лево (5) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень 9/Б Лево (6) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень 9/М Низ (7) (°C)"),
                parseCSVtoDouble(csvRecord, "Li мишень 9/Б Низ (8) (°C)"),
                parseCSVtoDouble(csvRecord, "Боп-1М/Б2/Зал/Гамма (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "Боп-1М/Б2/Зал/Нейтроны (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "Боп-1М/Б2/Кор/Гамма (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "Боп-1М/Б2/Кор/Нейтроны (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "Боп-1М/Б3/Зал/Гамма (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "Боп-1М/Б3/Зал/Нейтроны (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "Боп-1М/Б3/Кор/Гамма (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "Боп-1М/Б3/Кор/Нейтроны (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "Li6/Нейтроны (шт/сек)"),
                parseCSVtoDouble(csvRecord, "Li6/Гамма (шт/сек)"),
                parseCSVtoDouble(csvRecord, "Li6/Загрузка (%)"),
                parseCSVtoDouble(csvRecord, "Li6/Интеграл нейтронов (шт)"),
                parseCSVtoDouble(csvRecord, "Эфф. обдирки/Нейтралы I (мА)"),
                parseCSVtoDouble(csvRecord, "Эфф. обдирки/Эфф. (%)"),
                parseCSVtoDouble(csvRecord, "UltravoltPs/300V/U set (В)"),
                parseCSVtoDouble(csvRecord, "UltravoltPs/300V/I set (мА)"),
                parseCSVtoDouble(csvRecord, "UltravoltPs/300V/U get (В)"),
                parseCSVtoDouble(csvRecord, "UltravoltPs/300V/I get (мА)"),
                parseCSVtoDouble(csvRecord, "БДН/A/n (шт/сек)"),
                parseCSVtoDouble(csvRecord, "БДН/A/g (шт/сек)"),
                parseCSVtoDouble(csvRecord, "БДН/A/gn (шт/сек)"),
                parseCSVtoDouble(csvRecord, "БДН/A/gSv (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "БДН/A/nSv (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "БДН/B/n (шт/сек)"),
                parseCSVtoDouble(csvRecord, "БДН/B/g (шт/сек)"),
                parseCSVtoDouble(csvRecord, "БДН/B/gn (шт/сек)"),
                parseCSVtoDouble(csvRecord, "БДН/B/gSv (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "БДН/B/nSv (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "БДН/C/n (шт/сек)"),
                parseCSVtoDouble(csvRecord, "БДН/C/g (шт/сек)"),
                parseCSVtoDouble(csvRecord, "БДН/C/gn (шт/сек)"),
                parseCSVtoDouble(csvRecord, "БДН/C/gSv (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "БДН/C/nSv (Зв/Ч)"),
                parseCSVtoDouble(csvRecord, "Обдир миш/Слив (°C)"),
                parseCSVtoDouble(csvRecord, "Обдир миш/Напор (°C)"),
                parseCSVtoDouble(csvRecord, "Обдир миш/Мощность (Вт)"),
                parseCSVtoDouble(csvRecord, "Обдир миш/Поток (л/мин)"),
                parseCSVtoDouble(csvRecord, "Обдир миш/Аргон"),
                parseCSVtoDouble(csvRecord, "HPGe/Мёртвое время (%)"),
                parseCSVtoDouble(csvRecord, "HPGe/Скорость в интеграле (шт/сек)"),
                parseCSVtoDouble(csvRecord, "HPGe/Интеграл (шт)"),
                parseCSVtoDouble(csvRecord, "Вычислятор флюенса (Bergoz/HEBL/Ток)/Флюенс (мАч)"),
                parseCSVtoDouble(csvRecord, "Вычислятор флюенса (ЭЛВ/Beam0)/Флюенс (мАч)"),
                parseCSVtoDouble(csvRecord, "Вычислятор флюенса (Боп-1М/Б2/Зал/Гамма)/Флюенс (мАч)"),
                parseCSVtoDouble(csvRecord, "Вычислятор флюенса (Боп-1М/Б2/Зал/Нейтроны)/Флюенс (мАч)"),
                parseCSVtoDouble(csvRecord, "Вычислятор флюенса (Li6/Нейтроны)/Флюенс (мАч)"),
                parseCSVtoDouble(csvRecord, "Ускоритель/Входная охлаждаемая диафрагма/Слив (°C)"),
                parseCSVtoDouble(csvRecord, "Ускоритель/Входная охлаждаемая диафрагма/Напор (°C)"),
                parseCSVtoDouble(csvRecord, "Ускоритель/Входная охлаждаемая диафрагма/Мощность (Вт)"),
                parseCSVtoDouble(csvRecord, "Ускоритель/Входная охлаждаемая диафрагма/Поток (л/мин)"),
                parseCSVtoDouble(csvRecord, "Вакуум/Ускоритель выход (Па)"),
                parseCSVtoDouble(csvRecord, "Вакуум/HEBL После развертки (Па)"),
                operatorDateAndTime,
                csvRecord.get("journal")
            )

            resultData.add(data)
        }

        return resultData
    }

    fun getActualHeaders(path: String): MutableSet<String> {
        val reader = BufferedReader(InputStreamReader(FileInputStream(path), "Windows-1251"))

        val csvParser = CSVParser(
            reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withDelimiter('	')
        )

        return csvParser.headerMap.keys

    }

    fun getFileSizeKiloBytes(path: String): Long {
        val file = File(path)
        return file.length() / 1024
    }

    @GetMapping("/start")
    fun start(): ResponseEntity<String> {

        val headers: MutableSet<String>
        val files = TreeMap<String, String>()
        val pathName = "D:\\BNCT_DATA"

        File(pathName).walk().forEach {
            if (it.isFile) {
                if (filesRepository.findById(it.name).isEmpty) {
                    files[it.name] = it.path
                }
            }
        }

        headers = getActualHeaders(files.getValue(files.lastKey()))


        val yellow = "\u001B[33m"
        val reset = "\u001B[0m"

        for ((name, path) in files) {
            val fileTimeout = System.currentTimeMillis()
            println("$yellow${SimpleDateFormat("hh:mm:ss").format(Date())}:$reset Начинаем работу " +
                    "с файлом $name")

            val resultData = getFileData(name, path, headers)

            println(
                "$yellow${SimpleDateFormat("hh:mm:ss").format(Date())}:$reset " +
                        "всего обнаружено ${resultData.size} записей, начинаем перенос в базу данных"
            )

            //val saveInDbTimeoutList = mutableListOf<Long>()

            val existData: MutableList<BnctData> = mutableListOf()

            for (data in resultData) {
                if (bnctRepository.findById(data.Polnaia_data!!).isPresent) {
                    existData.add(data)
                    println("$yellow${SimpleDateFormat("hh:mm:ss").format(Date())}:$reset В базе данных " +
                            "уже существует запись с id: ${data.Polnaia_data}")
                }
            }

            resultData.removeAll(existData)

            //val saveTimeout = System.currentTimeMillis()
            bnctRepository.saveAll(resultData)
            //saveInDbTimeoutList.add(System.currentTimeMillis() - saveTimeout)

//            for (data in resultData) {
//
//                if (bnctRepository.findById(data.Polnaia_data!!).isEmpty) {
//                    val saveTimeout = System.currentTimeMillis()
//                    bnctRepository.save(data)
//                    saveinDbTimeoutList.add(System.currentTimeMillis() - saveTimeout)
//                } else {
//                    println("$yellow${SimpleDateFormat("hh:mm:ss").format(Date())}:$reset В базе данных уже существует запись с id: ${data.Polnaia_data}")
//                }
//            }

            filesRepository.save(FileProcessed(name))

//            var saveInDbResultTimeout: Long = 0

//            for (time in saveInDbTimeoutList) {
//                saveInDbResultTimeout += time
//            }

            //saveInDbResultTimeout /= saveInDbTimeoutList.size
            val fileResultTimeout: Long = System.currentTimeMillis() - fileTimeout

            println(
                "$yellow${SimpleDateFormat("hh:mm:ss").format(Date())}:$reset Закончили перенос файла $name"
            )
            println("---||Размер файла: ${getFileSizeKiloBytes(path)} Kb")
            println(
                "---||Общее время переноса данных $fileResultTimeout миллисекунд " +
                        "(${TimeUnit.MILLISECONDS.toMinutes(fileResultTimeout)} мин.)"
            )
            //println("---||Среднее время записи одной строки: $saveInDbResultTimeout миллисекунд")
        }

        return ResponseEntity.ok("move to DB is done")
    }
}