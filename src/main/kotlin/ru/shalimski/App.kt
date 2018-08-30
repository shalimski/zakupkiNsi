package ru.shalimski

import org.apache.commons.io.IOUtils
import org.apache.commons.net.ftp.FTP
import org.apache.commons.net.ftp.FTPClient
import org.json.JSONArray
import org.json.JSONObject
import org.json.XML
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.sql.DriverManager
import java.util.*
import java.util.zip.ZipFile


fun main(args: Array<String>) {

    println(" ftp.zakupki.gov.ru - download nsi")

    var onlydownload = false
    var onlyparse = false

    if (args.isNotEmpty()){
        when (args[0]){
            "--help" -> {
                println(" -d - only download zip files from ftp server \n -p - only parse zip files into postgresql" )
                return}
            "-d" -> onlydownload = true
            "-p" -> onlyparse = true
        }
    }

    val connectionString: String
    val username: String
    val password: String

    try {
        val prop = Properties()
        val inputStream = FileInputStream(Constants.PROPERTY_FILE)
        prop.load(inputStream)

        val host = prop.getProperty(Constants.HOST, "localhost")
        val port = prop.getProperty(Constants.PORT, "5432")
        val database = prop.getProperty(Constants.DATABASE, "testbase")
        username = prop.getProperty(Constants.USER, "postgres")
        password = prop.getProperty(Constants.PASS, "")

        connectionString = "jdbc:postgresql://$host:$port/$database"

    } catch (e: Exception) {
        println("Error in config file ${Constants.PROPERTY_FILE}")
        e.printStackTrace()
        return
    }

    if (!onlyparse) downloadOrganizations()
    if (onlydownload) return

    val zipFiles = File(Constants.FILEPATH).listFiles { _, name -> name.contains(".zip") }

    zipFiles.sortBy { file -> file.name }

    val connection = DriverManager.getConnection(connectionString, username, password)
    val stat = connection.createStatement()
    stat.execute(createTable())
    stat.close()

    zipFiles.forEach { file ->
        val zin = ZipFile(file.absoluteFile)
        val entry = zin.entries().nextElement()
        val input = zin.getInputStream(entry)
        val xmlString = IOUtils.toString(input, Charsets.UTF_8)
        val xmlJSONObj = XML.toJSONObject(xmlString, true)
        val arrayOrg = xmlJSONObj.query(Constants.XML_ORGANIZATION) as JSONArray

        val st = connection.createStatement()
        connection.autoCommit = false

        arrayOrg.forEach {
            it as JSONObject
            st.addBatch(updateExpression(it.get("oos:regNumber").toString(), it.toString().replace("\"oos:", "\"").replace("'", "''")))
        }

        try {
            println("parsing file ${zin.name}")
            st.executeBatch()
            connection.commit()
            file.delete()
            st.close()
            zin.close()

        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    connection.close()

}

fun downloadOrganizations() {

    val ftp = FTPClient()

    with(ftp) {
        connect(Constants.FTP)
        login(Constants.FTP_44, Constants.FTP_44)
        enterLocalPassiveMode()
        setFileType(FTP.BINARY_FILE_TYPE)
        bufferSize = 1000
        changeWorkingDirectory(Constants.XML_DIRECTORY)
    }

    val files = ftp.listFiles("*.zip")

    files.forEach {
        val outputStream = BufferedOutputStream(FileOutputStream(Constants.FILEPATH + File.separator + it.name))
        ftp.retrieveFile(it.name, outputStream)
        outputStream.close()
        println("download file " + it.name)
    }

    ftp.disconnect()

}

fun updateExpression(id: String, data: String): String =
        "INSERT INTO organizations (id, data) VALUES ('$id' , '$data') ON CONFLICT (id) DO UPDATE SET data='$data'"

fun createTable(): String {
    return """CREATE TABLE IF NOT EXISTS public.organizations
    (
        id character varying(11) COLLATE pg_catalog."default" NOT NULL,
        data jsonb NOT NULL,
        CONSTRAINT organizations_pkey PRIMARY KEY (id)
    )"""
}


object Constants {
    val PROPERTY_FILE = System.getProperty("user.dir") + File.separator + "config.properties"
    const val HOST = "host"
    const val PORT = "port"
    const val DATABASE = "database"
    const val USER = "user"
    const val PASS = "pass"
    const val FTP = "ftp.zakupki.gov.ru"
    const val FTP_44 = "free"
    val FILEPATH: String = File("").absolutePath
    const val XML_ORGANIZATION = "/export/nsiOrganizationList/nsiOrganization"
    const val XML_DIRECTORY = "fcs_nsi/nsiOrganization/"
}
