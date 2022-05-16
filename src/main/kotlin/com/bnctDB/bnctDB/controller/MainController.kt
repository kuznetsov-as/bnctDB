package com.bnctDB.bnctDB.controller

import com.bnctDB.bnctDB.model.BnctData
import com.bnctDB.bnctDB.model.FileProcessed
import com.bnctDB.bnctDB.repository.BnctRepository
import com.bnctDB.bnctDB.repository.FilesRepository
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.jetbrains.annotations.NotNull
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.*
import java.util.concurrent.TimeUnit


@Controller
@RequestMapping("bnct")
class MainController(val bnctRepository: BnctRepository, val filesRepository: FilesRepository) {

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

    fun recordToDouble(record: String): Double? {
        return try {
            BigDecimal(record.replace(",", ".")).toDouble()
        } catch (e: NumberFormatException) {
            null
        }
    }

    fun parseDate(@NotNull date: String): LocalDateTime? {
        return try {
            LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd@HH:mm:ss.SSS"))
        } catch (e: DateTimeParseException) {
            println("Не удалось распарсить $date, строка для этого времени будет пропущена")
            return null
        }
    }

    fun parseOperatorDate(date: String): LocalDateTime? {
        return try {
            LocalDateTime.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd--HH-mm-ss"))
        } catch (e: DateTimeParseException) {
            return null
        }
    }

    fun getFileData(name: String, path: String, headers: MutableSet<String>): MutableList<BnctData> {
        val reader = BufferedReader(InputStreamReader(FileInputStream(path), "Windows-1251"))

        val csvParser = CSVParser(
            reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withDelimiter('	')
        )

        val resultData = mutableListOf<BnctData>()

        for (csvRecord in csvParser) {
            val dateAndTime = parseDate(csvRecord.get("Полная дата")) ?: continue

            var operatorDateAndTime: LocalDateTime? = null
            if (csvRecord.get("operatorPcDT").isNotEmpty()) {
                operatorDateAndTime = parseOperatorDate(csvRecord.get("operatorPcDT"))
            }

            val data = BnctData(
                dateAndTime,
                recordToDouble(csvRecord.get("H-/Конус (мА)")),
                recordToDouble(csvRecord.get("H-/Линза Воблова/Слив (°C)")),
                recordToDouble(csvRecord.get("H-/Линза Воблова/Напор (°C)")),
                recordToDouble(csvRecord.get("H-/Линза Воблова/Мощность (Вт)")),
                recordToDouble(csvRecord.get("H-/Линза Воблова/Поток (л/мин)")),
                recordToDouble(csvRecord.get("H-/Напор IFM (л/мин)")),
                recordToDouble(csvRecord.get("H-/Слив IFM (л/мин)")),
                recordToDouble(csvRecord.get("H-/Источник (Па)")),
                recordToDouble(csvRecord.get("H-/Дифф. откачка (Па)")),
                recordToDouble(csvRecord.get("Bergoz/HEBL/Ток (мА)")),
                recordToDouble(csvRecord.get("Конус/1/Ток (мА)")),
                recordToDouble(csvRecord.get("Конус/1/Центр (°C)")),
                recordToDouble(csvRecord.get("Конус/1/М Верх (°C)")),
                recordToDouble(csvRecord.get("Конус/1/М Право (°C)")),
                recordToDouble(csvRecord.get("Конус/1/М Низ (°C)")),
                recordToDouble(csvRecord.get("Конус/1/М Лево (°C)")),
                recordToDouble(csvRecord.get("Конус/1/Б Верх (°C)")),
                recordToDouble(csvRecord.get("Конус/1/Б Право (°C)")),
                recordToDouble(csvRecord.get("Конус/1/Б Низ (°C)")),
                recordToDouble(csvRecord.get("Конус/1/Б Лево (°C)")),
                recordToDouble(csvRecord.get("Конус/2/Ток (мА)")),
                recordToDouble(csvRecord.get("Конус/2/Центр (°C)")),
                recordToDouble(csvRecord.get("Конус/2/М Верх (°C)")),
                recordToDouble(csvRecord.get("Конус/2/М Право (°C)")),
                recordToDouble(csvRecord.get("Конус/2/М Низ (°C)")),
                recordToDouble(csvRecord.get("Конус/2/М Лево (°C)")),
                recordToDouble(csvRecord.get("Конус/2/Б Верх (°C)")),
                recordToDouble(csvRecord.get("Конус/2/Б Право (°C)")),
                recordToDouble(csvRecord.get("Конус/2/Б Низ (°C)")),
                recordToDouble(csvRecord.get("Конус/2/Б Лево (°C)")),
                recordToDouble(csvRecord.get("ЭЛВ/Энергия (кэВ)")),
                recordToDouble(csvRecord.get("ЭЛВ/Beam0 (мА)")),
                recordToDouble(csvRecord.get("ЭЛВ/EnergyU get (кВ)")),
                recordToDouble(csvRecord.get("ЭЛВ/EnergyU set (кВ)")),
                recordToDouble(csvRecord.get("ЭЛВ/EnergyI (мА)")),
                recordToDouble(csvRecord.get("ЭЛВ/Avarage (мА)")),
                recordToDouble(csvRecord.get("ЭЛВ/Sec1U (кВ)")),
                recordToDouble(csvRecord.get("ЭЛВ/Sec1I (мА)")),
                recordToDouble(csvRecord.get("ЭЛВ/IsolatorU (кВ)")),
                recordToDouble(csvRecord.get("ЭЛВ/IsolatorI (мА)")),
                recordToDouble(csvRecord.get("ЭЛВ/Dark (мА)")),
                recordToDouble(csvRecord.get("Охл. диаф./1/Верх (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./1/Право (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./1/Низ (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./1/Лево (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./2/Верх (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./2/Право (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./2/Низ (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./2/Лево (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./3/Верх (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./3/Право (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./3/Низ (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./3/Лево (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./4/Верх (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./4/Право (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./4/Низ (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./4/Лево (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./5/Верх (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./5/Право (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./5/Низ (°C)")),
                recordToDouble(csvRecord.get("Охл. диаф./5/Лево (°C)")),
                recordToDouble(csvRecord.get("Li мишень/Ток (мА)")),
                recordToDouble(csvRecord.get("Li мишень/Центр (0) (°C)")),
                recordToDouble(csvRecord.get("Li мишень/Низ-право (1) (°C)")),
                recordToDouble(csvRecord.get("Li мишень/Право (2) (°C)")),
                recordToDouble(csvRecord.get("Li мишень/Верх-право (3) (°C)")),
                recordToDouble(csvRecord.get("Li мишень/Верх (4) (°C)")),
                recordToDouble(csvRecord.get("Li мишень/Верх-лево (5) (°C)")),
                recordToDouble(csvRecord.get("Li мишень/Лево (6) (°C)")),
                recordToDouble(csvRecord.get("Li мишень/Низ-лево (7) (°C)")),
                recordToDouble(csvRecord.get("Li мишень/Вода вход (л/мин)")),
                recordToDouble(csvRecord.get("Li мишень/Вода выход 1 (л/мин)")),
                recordToDouble(csvRecord.get("Li мишень/Вода выход 2 (л/мин)")),
                recordToDouble(csvRecord.get("Li мишень/Вакуум (Па)")),
                recordToDouble(csvRecord.get("Li мишень 9/Центр (0) (°C)")),
                recordToDouble(csvRecord.get("Li мишень 9/М Право (1) (°C)")),
                recordToDouble(csvRecord.get("Li мишень 9/Б Право (2) (°C)")),
                recordToDouble(csvRecord.get("Li мишень 9/М Верх (3) (°C)")),
                recordToDouble(csvRecord.get("Li мишень 9/Б Верх (4) (°C)")),
                recordToDouble(csvRecord.get("Li мишень 9/М Лево (5) (°C)")),
                recordToDouble(csvRecord.get("Li мишень 9/Б Лево (6) (°C)")),
                recordToDouble(csvRecord.get("Li мишень 9/М Низ (7) (°C)")),
                recordToDouble(csvRecord.get("Li мишень 9/Б Низ (8) (°C)")),
                recordToDouble(csvRecord.get("Боп-1М/Б2/Зал/Гамма (Зв/Ч)")),
                recordToDouble(csvRecord.get("Боп-1М/Б2/Зал/Нейтроны (Зв/Ч)")),
                recordToDouble(csvRecord.get("Боп-1М/Б2/Кор/Гамма (Зв/Ч)")),
                recordToDouble(csvRecord.get("Боп-1М/Б2/Кор/Нейтроны (Зв/Ч)")),
                recordToDouble(csvRecord.get("Боп-1М/Б3/Зал/Гамма (Зв/Ч)")),
                recordToDouble(csvRecord.get("Боп-1М/Б3/Зал/Нейтроны (Зв/Ч)")),
                recordToDouble(csvRecord.get("Боп-1М/Б3/Кор/Гамма (Зв/Ч)")),
                recordToDouble(csvRecord.get("Боп-1М/Б3/Кор/Нейтроны (Зв/Ч)")),
                recordToDouble(csvRecord.get("Li6/Нейтроны (шт/сек)")),
                recordToDouble(csvRecord.get("Li6/Гамма (шт/сек)")),
                recordToDouble(csvRecord.get("Li6/Загрузка (%)")),
                recordToDouble(csvRecord.get("Li6/Интеграл нейтронов (шт)")),
                recordToDouble(csvRecord.get("Эфф. обдирки/Нейтралы I (мА)")),
                recordToDouble(csvRecord.get("Эфф. обдирки/Эфф. (%)")),
                recordToDouble(csvRecord.get("UltravoltPs/300V/U set (В)")),
                recordToDouble(csvRecord.get("UltravoltPs/300V/I set (мА)")),
                recordToDouble(csvRecord.get("UltravoltPs/300V/U get (В)")),
                recordToDouble(csvRecord.get("UltravoltPs/300V/I get (мА)")),
                recordToDouble(csvRecord.get("БДН/A/n (шт/сек)")),
                recordToDouble(csvRecord.get("БДН/A/g (шт/сек)")),
                recordToDouble(csvRecord.get("БДН/A/gn (шт/сек)")),
                recordToDouble(csvRecord.get("БДН/A/gSv (Зв/Ч)")),
                recordToDouble(csvRecord.get("БДН/A/nSv (Зв/Ч)")),
                recordToDouble(csvRecord.get("БДН/B/n (шт/сек)")),
                recordToDouble(csvRecord.get("БДН/B/g (шт/сек)")),
                recordToDouble(csvRecord.get("БДН/B/gn (шт/сек)")),
                recordToDouble(csvRecord.get("БДН/B/gSv (Зв/Ч)")),
                recordToDouble(csvRecord.get("БДН/B/nSv (Зв/Ч)")),
                recordToDouble(csvRecord.get("БДН/C/n (шт/сек)")),
                recordToDouble(csvRecord.get("БДН/C/g (шт/сек)")),
                recordToDouble(csvRecord.get("БДН/C/gn (шт/сек)")),
                recordToDouble(csvRecord.get("БДН/C/gSv (Зв/Ч)")),
                recordToDouble(csvRecord.get("БДН/C/nSv (Зв/Ч)")),
                recordToDouble(csvRecord.get("Обдир миш/Слив (°C)")),
                recordToDouble(csvRecord.get("Обдир миш/Напор (°C)")),
                recordToDouble(csvRecord.get("Обдир миш/Мощность (Вт)")),
                recordToDouble(csvRecord.get("Обдир миш/Поток (л/мин)")),
                recordToDouble(csvRecord.get("Обдир миш/Аргон")),
                recordToDouble(csvRecord.get("HPGe/Мёртвое время (%)")),
                recordToDouble(csvRecord.get("HPGe/Скорость в интеграле (шт/сек)")),
                recordToDouble(csvRecord.get("HPGe/Интеграл (шт)")),
                recordToDouble(csvRecord.get("Вычислятор флюенса (Bergoz/HEBL/Ток)/Флюенс (мАч)")),
                recordToDouble(csvRecord.get("Вычислятор флюенса (ЭЛВ/Beam0)/Флюенс (мАч)")),
                recordToDouble(csvRecord.get("Вычислятор флюенса (Боп-1М/Б2/Зал/Гамма)/Флюенс (мАч)")),
                recordToDouble(csvRecord.get("Вычислятор флюенса (Боп-1М/Б2/Зал/Нейтроны)/Флюенс (мАч)")),
                recordToDouble(csvRecord.get("Вычислятор флюенса (Li6/Нейтроны)/Флюенс (мАч)")),
                recordToDouble(csvRecord.get("Ускоритель/Входная охлаждаемая диафрагма/Слив (°C)")),
                recordToDouble(csvRecord.get("Ускоритель/Входная охлаждаемая диафрагма/Напор (°C)")),
                recordToDouble(csvRecord.get("Ускоритель/Входная охлаждаемая диафрагма/Мощность (Вт)")),
                recordToDouble(csvRecord.get("Ускоритель/Входная охлаждаемая диафрагма/Поток (л/мин)")),
                recordToDouble(csvRecord.get("Вакуум/Ускоритель выход (Па)")),
                recordToDouble(csvRecord.get("Вакуум/HEBL После развертки (Па)")),
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
        val pathName = "D:\\1"

        File(pathName).walk().forEach {
            if (it.isFile) {
                if (filesRepository.findById(it.name).isEmpty) {
                    files[it.name] = it.path
                }
            }
        }

        if (files.isNotEmpty()) {
            headers = getActualHeaders(files.getValue(files.lastKey()))
        } else {
            println("Не найдено новых файлов для переноса в бд")
            return ResponseEntity.ok("Не найдено новых файлов для переноса в бд")
        }


        val yellow = "\u001B[33m"
        val reset = "\u001B[0m"

        for ((name, path) in files) {
            val fileTimeout = System.currentTimeMillis()
            println(
                "$yellow${SimpleDateFormat("hh:mm:ss").format(Date())}:$reset Начинаем работу " +
                        "с файлом $name"
            )

            val resultData = getFileData(name, path, headers)

            println(
                "$yellow${SimpleDateFormat("hh:mm:ss").format(Date())}:$reset " +
                        "всего обнаружено ${resultData.size} записей"
            )

            //val saveInDbTimeoutList = mutableListOf<Long>()

            val existData: MutableList<BnctData> = mutableListOf()

            for (data in resultData) {
                if (bnctRepository.findById(data.Polnaia_data!!).isPresent) {
                    existData.add(data)
                    println(
                        "$yellow${SimpleDateFormat("hh:mm:ss").format(Date())}:$reset В базе данных " +
                                "уже существует запись с id: ${data.Polnaia_data}"
                    )
                }
            }

            resultData.removeAll(existData)

            println(
                "$yellow${SimpleDateFormat("hh:mm:ss").format(Date())}:$reset " +
                        "завершена проверка наличия данных в бд, начинаем перенос в базу данных"
            )

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

        println(
            "$yellow${SimpleDateFormat("hh:mm:ss").format(Date())}:$reset " +
                    "Перенос данных завершен"
        )
        return ResponseEntity.ok("move to DB is done")
    }
}