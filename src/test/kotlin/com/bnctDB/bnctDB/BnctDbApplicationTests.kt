package com.bnctDB.bnctDB

import com.bnctDB.bnctDB.controller.MainController
import com.bnctDB.bnctDB.repository.BnctRepository
import com.bnctDB.bnctDB.repository.FilesRepository
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.Mock
import org.springframework.boot.test.context.SpringBootTest
import java.util.concurrent.TimeUnit


@SpringBootTest
class BnctDbApplicationTests {

    @Mock
    lateinit var bnctRepository: BnctRepository

    @Mock
    lateinit var filesRepository: FilesRepository

    @Test
    fun parseScientificNotationTest() {

        var result = MainController(bnctRepository, filesRepository).recordToDouble("0,0e+00")
        var actual = 0.0
        assertEquals(actual, result)

        result = MainController(bnctRepository, filesRepository).recordToDouble("-2,594e-03")
        actual = -0.002594
        assertEquals(actual, result)

        result = MainController(bnctRepository, filesRepository).recordToDouble("3,387e-04")
        actual = 3.387E-4
        assertEquals(actual, result)

        result = MainController(bnctRepository, filesRepository).recordToDouble("nan")
        assertNull(result)
    }

    @Test
    fun parseSimpleTest() {

        var result = MainController(bnctRepository, filesRepository).recordToDouble("0,000")
        var actual = 0.0
        assertEquals(actual, result)

        result = MainController(bnctRepository, filesRepository).recordToDouble("0")
        assertEquals(actual, result)

        result = MainController(bnctRepository, filesRepository).recordToDouble("0,7")
        actual = 0.7
        assertEquals(actual, result)

        result = MainController(bnctRepository, filesRepository).recordToDouble("-0,008")
        actual = -0.008
        assertEquals(actual, result)

        result = MainController(bnctRepository, filesRepository).recordToDouble("-24,443")
        actual = -24.443
        assertEquals(actual, result)
    }

    @Test
    fun parseDateTest() {

        var result = MainController(bnctRepository, filesRepository).parseDate("2022-04-29@10:23:23.565")
        assertTrue(result != null)
        if (result != null) {
            assertEquals(2022, result.year)
            assertEquals(4, result.monthValue)
            assertEquals(29, result.dayOfMonth)
            assertEquals(10, result.hour)
            assertEquals(23, result.minute)
            assertEquals(23, result.second)
            assertEquals(565, TimeUnit.NANOSECONDS.toMillis(result.nano.toLong()))
        }

        result = MainController(bnctRepository, filesRepository).parseDate("nan")
        assertTrue(result == null)
    }

    @Test
    fun parseOperatorDateTest() {

        var result = MainController(bnctRepository, filesRepository).parseOperatorDate("2022-04-29--09-58-17")
        assertTrue(result != null)
        if (result != null) {
            assertEquals(2022, result.year)
            assertEquals(4, result.monthValue)
            assertEquals(29, result.dayOfMonth)
            assertEquals(9, result.hour)
            assertEquals(58, result.minute)
            assertEquals(17, result.second)
        }

        result = MainController(bnctRepository, filesRepository).parseOperatorDate("nan")
        assertTrue(result == null)

        result = MainController(bnctRepository, filesRepository).parseOperatorDate("")
        assertTrue(result == null)
    }
}
