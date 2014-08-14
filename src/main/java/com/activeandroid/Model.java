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

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import com.activeandroid.annotation.ForeignKey;
import com.activeandroid.annotation.PrimaryKey;
import com.activeandroid.content.ContentProvider;
import com.activeandroid.query.Delete;
import com.activeandroid.query.Select;
import com.activeandroid.serializer.TypeSerializer;
import com.activeandroid.util.AALog;
import com.activeandroid.util.ReflectionUtils;
import com.activeandroid.util.SQLiteUtils;

import java.lang.reflect.Field;
import java.util.List;

@SuppressWarnings("unchecked")
public abstract class Model implements IModel{

	private TableInfo mTableInfo;

	public Model() {
		mTableInfo = Cache.getTableInfo(getClass());
	}

    private long mId;

    /**
     * Use This method to return the values of your primary key, must be separated by comma delimiter in order of declaration
     * Also each object thats instance of {@link java.lang.Number} must be DataBaseUtils.sqlEscapeString(object.toString)
     * @return
     */
	public abstract String getId();

    @Override
	public final void delete() {
		SQLiteUtils.delete(this);
	}

    @Override
	public final void save() {
        SQLiteUtils.save(this);
	}

    public final void save(int mode) {
        SQLiteUtils.save(this, mode);
    }

    @Override
    public boolean exists(){
        return SQLiteUtils.exists(this);
    }

    public void update(){

    }

    @Override
	public final void loadFromCursor(Cursor cursor) {
        SQLiteUtils.loadFromCursor(cursor, this);
	}

	@Override
	public String toString() {
		return mTableInfo!=null? mTableInfo.getTableName() + "@" + getId() : "No Table for: " + getClass() + "@" + getId();
	}

    @Override
    public long getRowId(){
        return mId;
    }

    @Override
    public void setRowId(long id) {
        this.mId = id;
    }
}
