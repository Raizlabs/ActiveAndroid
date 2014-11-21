package com.activeandroid;

/*
 * Copyright (C) 2010 Michael Pardo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import com.activeandroid.manager.SingleDBManager;
import com.activeandroid.runtime.DBRequest;
import com.activeandroid.runtime.DBRequestInfo;
import com.activeandroid.util.AALog;
import com.activeandroid.util.NaturalOrderComparator;
import com.activeandroid.util.SQLiteUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DatabaseHelper extends SQLiteOpenHelper {
	//////////////////////////////////////////////////////////////////////////////////////
	// PUBLIC CONSTANTS
	//////////////////////////////////////////////////////////////////////////////////////

	public final static String MIGRATION_PATH = "migrations";

    public final static String TEMP_DB_NAME = "temp-";

    private SQLiteOpenHelper mTempDatabase;

    private String mDatabaseName;

    DatabaseHelperListener mListener;

	//////////////////////////////////////////////////////////////////////////////////////
	// CONSTRUCTORS
	//////////////////////////////////////////////////////////////////////////////////////

	public DatabaseHelper(Configuration configuration) {
		super(configuration.getContext(), configuration.getDatabaseName(), null, configuration.getDatabaseVersion());
        mDatabaseName = configuration.getDatabaseName();
		copyAttachedDatabase(configuration.getContext(), configuration.getDatabaseName(), configuration.getDatabaseName());

        // temporary database uses the same methods
        mTempDatabase = new SQLiteOpenHelper(configuration.getContext(), TEMP_DB_NAME + configuration.getDatabaseName(),
                null, configuration.getDatabaseVersion()) {

            @Override
            public void onOpen(SQLiteDatabase db) {
                executePragmas(db);
            }

            @Override
            public void onCreate(SQLiteDatabase db) {
                executePragmas(db);
                executeCreate(db);
                executeMigrations(db, -1, db.getVersion());
            }

            @Override
            public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
                executePragmas(db);
                executeCreate(db);
                executeMigrations(db, oldVersion, newVersion);
            }
        };
        // see if we need to make this mirror the prepackaged
        restoreDatabase(configuration.getContext(), TEMP_DB_NAME + configuration.getDatabaseName(), configuration.getDatabaseName());
        mTempDatabase.getWritableDatabase();
	}

    public void setListener(DatabaseHelperListener listener){
        mListener = listener;
    }

	//////////////////////////////////////////////////////////////////////////////////////
	// OVERRIDEN METHODS
	//////////////////////////////////////////////////////////////////////////////////////

	@Override
	public void onOpen(SQLiteDatabase db) {
        if(mListener!=null){
            mListener.onOpen(db);
        }
		executePragmas(db);
	}

	@Override
	public void onCreate(SQLiteDatabase db) {
        if(mListener!=null){
            mListener.onCreate(db);
        }
		executePragmas(db);
		executeCreate(db);
		executeMigrations(db, -1, db.getVersion());
	}

	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        if(mListener!=null){
            mListener.onUpgrade(db, oldVersion, newVersion);
        }
		executePragmas(db);
		executeCreate(db);
		executeMigrations(db, oldVersion, newVersion);
	}

	//////////////////////////////////////////////////////////////////////////////////////
	// PUBLIC METHODS
	//////////////////////////////////////////////////////////////////////////////////////

	public void copyAttachedDatabase(Context context, String databaseName, String prepackagedName) {
		final File dbPath = context.getDatabasePath(databaseName);

		// If the database already exists, return
		if (dbPath.exists()) {
			return;
		}

		// Make sure we have a path to the file
		dbPath.getParentFile().mkdirs();

		// Try to copy database file
        try {
            writeDB(dbPath, context.getAssets().open(prepackagedName));
        } catch (IOException e) {
            AALog.e(e.getMessage());
        }
    }

    public void restoreDatabase(Context context, String databaseName, String prepackagedName) {
        final File dbPath = context.getDatabasePath(databaseName);

        // If the database already exists, return
        if (dbPath.exists()) {
            return;
        }

        // Make sure we have a path to the file
        dbPath.getParentFile().mkdirs();

        // Try to copy database file
        try {
            // check existing and use that as backup
            File existingDb = context.getDatabasePath(getDatabaseName());
            InputStream inputStream;
            // if it exists and the integrity is ok
            if(existingDb.exists() && Cache.checkDbIntegrity(this)) {
                inputStream = new FileInputStream(existingDb);
            } else {
                inputStream = context.getAssets().open(prepackagedName);
            }
            writeDB(dbPath, inputStream);
        } catch (IOException e) {
            AALog.e(e.getMessage());
        }
    }

    private void writeDB(File dbPath, InputStream existingDB) {
        try {
            final OutputStream output = new FileOutputStream(dbPath);

            byte[] buffer = new byte[1024];
            int length;

            while ((length = existingDB.read(buffer)) > 0) {
                output.write(buffer, 0, length);
            }

            output.flush();
            output.close();
            existingDB.close();
        }
        catch (IOException e) {
            AALog.e("Failed to open file", e);
        }
    }

    public String getDatabaseName() {
        return mDatabaseName;
    }

    /**
     * If integrity check fails, this method will use the backup db.
     * @param context
     */
    public void restoreBackUp(Context context) {
        File db = context.getDatabasePath(TEMP_DB_NAME + mDatabaseName);
        File corrupt = context.getDatabasePath(mDatabaseName);
        if(corrupt.delete()) {
            try {
                 writeDB(corrupt,  new FileInputStream(db));
            } catch (IOException e) {
                AALog.e(e.getMessage());
            }
        } else {
            AALog.e("Failed to delete DB");
        }

    }

	//////////////////////////////////////////////////////////////////////////////////////
	// PRIVATE METHODS
	//////////////////////////////////////////////////////////////////////////////////////

	protected void executePragmas(SQLiteDatabase db) {
		if (SQLiteUtils.FOREIGN_KEYS_SUPPORTED) {
			db.execSQL("PRAGMA foreign_keys=ON;");
			AALog.i("Foreign Keys supported. Enabling foreign key features.");
		}
	}

	protected void executeCreate(SQLiteDatabase db) {
		db.beginTransaction();
		try {
			for (TableInfo tableInfo : Cache.getTableInfos()) {
				db.execSQL(SQLiteUtils.createTableDefinition(tableInfo));
			}
			db.setTransactionSuccessful();
		}
		finally {
			db.endTransaction();
		}
	}

	protected boolean executeMigrations(SQLiteDatabase db, int oldVersion, int newVersion) {
		boolean migrationExecuted = false;
		try {
			final List<String> files = Arrays.asList(Cache.getContext().getAssets().list(MIGRATION_PATH));
			Collections.sort(files, new NaturalOrderComparator());

			db.beginTransaction();
			try {
				for (String file : files) {
					try {
						final int version = Integer.valueOf(file.replace(".sql", ""));

						if (version > oldVersion && version <= newVersion) {
							executeSqlScript(db, file);
							migrationExecuted = true;

							AALog.i(file + " executed succesfully.");
						}
					}
					catch (NumberFormatException e) {
						AALog.w("Skipping invalidly named file: " + file, e);
					}
				}
				db.setTransactionSuccessful();
			}
			finally {
				db.endTransaction();
			}
		}
		catch (IOException e) {
			AALog.e("Failed to execute migrations.", e);
		}

		return migrationExecuted;
	}

	protected void executeSqlScript(SQLiteDatabase db, String file) {
		try {
			final InputStream input = Cache.getContext().getAssets().open(MIGRATION_PATH + "/" + file);
			final BufferedReader reader = new BufferedReader(new InputStreamReader(input));
			String line = null;

			while ((line = reader.readLine()) != null) {
				db.execSQL(line.replace(";", ""));
			}
		}
		catch (IOException e) {
			AALog.e("Failed to execute " + file, e);
		}
	}

    /**
     * Saves the database as a backup
     */
    public void backupDB(final Context context) {
        SingleDBManager.getSharedInstance().getQueue().add(new DBRequest(DBRequestInfo.createFetch()) {
            @Override
            public void run() {
                File backup = context.getDatabasePath(TEMP_DB_NAME + mDatabaseName);
                if(backup.exists()) {
                    backup.delete();
                }
                File existing = context.getDatabasePath(mDatabaseName);

                try {
                    backup.getParentFile().mkdirs();
                    writeDB(backup, new FileInputStream(existing));
                } catch (FileNotFoundException e) {
                    AALog.e(e.getMessage());
                }
            }
        });

    }
}
