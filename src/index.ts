import dayjs from "dayjs";
import { Database, Statement } from "sqlite3";
import sqlite3 from "sqlite3";
import fs from "fs";
import utc from 'dayjs/plugin/utc'
import timezone from 'dayjs/plugin/timezone'

dayjs.extend(utc)
dayjs.extend(timezone)

/**
 * Run a query on SQLite3 database
 * @notExported
 * @param dbase Database used run query
 * @param sql The SQL command for run
 * @param params The SQL parameters
 * @returns Promise for executed query
 */
function runQuery(dbase: Database, sql: string, params: Array<void>) {
    return new Promise<any>((resolve, reject) => {
        return dbase.all(sql, params, (err: any, res: any) => {
            if (err) {
                reject(err.message);
            }
            resolve(res);
        });
    });
}

/**
 * Prepare a statement on SQLite3 database
 * @notExported
 * @param stmt Prepared statement for execute
 * @param params Parameters for statement
 * @returns Promise for executed statement
 */
function execStatement(stmt: Statement, params: (string | number)[]): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        stmt.run(params, (err: any, res: any) => {
            if (err) {
                reject(err.message);
            }
            resolve(res);
        });
    });
}

/**
 * Finalize a statement and commit executed SQLs 
 * @notExported
 * @param db Database for commit
 * @param stmt Statement to finalize
 * @returns Promise for finalizing and commit
 */
async function finalizeAndCommit(db: Database, stmt: Statement): Promise<void> {
    return new Promise<void>(async (resolve, reject) => {
        try {
            stmt.finalize();
            await runQuery(db, "COMMIT", []);
            resolve();
        } catch (err) {
            reject(err);
        }
    })
}

/**
 * Create database, table and statement
 * @notExported
 * @param dbFileName Name of the database file
 * @returns Promise for Database and Statement
 */
async function createNewDb(dbFileName: string): Promise<[Database, Statement]> {
    return new Promise<[Database, Statement]>(async (resolve, reject) => {
        try {
            let db = new Database(dbFileName, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE);
            await runQuery(db, "BEGIN", []);
            await runQuery(db, `CREATE TABLE "Measurements" ("id" INTEGER NOT NULL,"channel" INTEGER,"measured_value" REAL,"recorded_time" INTEGER, PRIMARY KEY("id" AUTOINCREMENT))`, []);
            let stmt = db.prepare("INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (?, ?, ?)");
            return resolve([db, stmt]);
        } catch (err) {
            reject(err);
        }
    })
}

/**
 * Insert measurements into table for 12 channels
 * @notExported
 * @param db Database for insertion
 * @param stmt Statement for executed
 * @param measuredValue Measured value
 * @param hourlyIterator Timestamp of measured values
 */
async function insertMeasurements(db: Database, stmt: Statement, measuredValue: number, hourlyIterator: dayjs.Dayjs): Promise<void> {
    let promises: Promise<void>[] = [];
    for (let channel = 1; channel <= 12; channel++) {
        promises.push(execStatement(stmt, [channel, measuredValue + (Math.round((Math.random() * 100) * 10) / 10), hourlyIterator.unix()]));
    }
    await Promise.all(promises);
}

/**
 * Generate measurements into SQLite DBs for testing
 * @notExported
 * @param year Starting year
 * @param generatedYears Number of years to generate
 * @param timeZone Timezone of insertion
 */
async function generateMeasurements(year: number, generatedYears: number, timeZone: string) {

    let hourlyIterator = dayjs(year + "01-01");
    let endOfYear = dayjs(year + generatedYears + "01-01");
    let measuredValue = 0;
    let db: Database | null = null;
    let stmt: Statement | null = null;
    while (hourlyIterator.isBefore(endOfYear) || hourlyIterator.isSame(endOfYear)) {
        if ((hourlyIterator.get("date") == 1) && (hourlyIterator.get("hour") == 0)) {
            if (db && stmt) {
                await finalizeAndCommit(db, stmt)
            }
            let dbFileName = hourlyIterator.format("YYYY-MM") + '-monthly.sqlite';
            if (fs.existsSync(dbFileName)) {
                fs.rmSync(dbFileName)
            }
            [db, stmt] = await createNewDb(dbFileName);
            console.log(dayjs().format(), `DB file '${dbFileName}' created.`);
        }

        if (db && stmt) {
            try {
                await insertMeasurements(db, stmt, measuredValue, hourlyIterator);
            } catch (err) {
                console.error(err);
            }
        }
        measuredValue += 100;
        hourlyIterator = hourlyIterator.add(1, "hour");
    }

    if (db && stmt) {
        try {
            await finalizeAndCommit(db, stmt);
        } catch (err) {
            console.error(err);
        }
    }
}

generateMeasurements(2022, 2, dayjs.tz.guess()).catch((reason) => {
    console.error(reason);
}).then(() => console.log("Factoring finished."));
