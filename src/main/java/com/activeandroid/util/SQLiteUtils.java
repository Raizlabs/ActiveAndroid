package com.activeandroid.util;

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
import android.database.DatabaseUtils;
import android.database.sqlite.SQLiteDatabase;
import android.os.Build;
import android.text.TextUtils;

import com.activeandroid.Cache;
import com.activeandroid.IModel;
import com.activeandroid.TableInfo;
import com.activeandroid.annotation.Column;
import com.activeandroid.annotation.ForeignKey;
import com.activeandroid.annotation.PrimaryKey;
import com.activeandroid.content.ContentProvider;
import com.activeandroid.exception.PrimaryKeyCannotBeNullException;
import com.activeandroid.query.Select;
import com.activeandroid.serializer.TypeSerializer;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public final class SQLiteUtils {
	//////////////////////////////////////////////////////////////////////////////////////
	// ENUMERATIONS
	//////////////////////////////////////////////////////////////////////////////////////

	public enum SQLiteType {
		INTEGER, REAL, TEXT, BLOB
	}

	//////////////////////////////////////////////////////////////////////////////////////
	// PUBLIC CONSTANTS
	//////////////////////////////////////////////////////////////////////////////////////

	public static final boolean FOREIGN_KEYS_SUPPORTED = Build.VERSION.SDK_INT >= Build.VERSION_CODES.FROYO;

	//////////////////////////////////////////////////////////////////////////////////////
	// PRIVATE CONTSANTS
	//////////////////////////////////////////////////////////////////////////////////////

	@SuppressWarnings("serial")
	private static final HashMap<Class<?>, SQLiteType> TYPE_MAP = new HashMap<Class<?>, SQLiteType>() {
		{
			put(byte.class, SQLiteType.INTEGER);
			put(short.class, SQLiteType.INTEGER);
			put(int.class, SQLiteType.INTEGER);
			put(long.class, SQLiteType.INTEGER);
			put(float.class, SQLiteType.REAL);
			put(double.class, SQLiteType.REAL);
			put(boolean.class, SQLiteType.INTEGER);
			put(char.class, SQLiteType.TEXT);
			put(byte[].class, SQLiteType.BLOB);
			put(Byte.class, SQLiteType.INTEGER);
			put(Short.class, SQLiteType.INTEGER);
			put(Integer.class, SQLiteType.INTEGER);
			put(Long.class, SQLiteType.INTEGER);
			put(Float.class, SQLiteType.REAL);
			put(Double.class, SQLiteType.REAL);
			put(Boolean.class, SQLiteType.INTEGER);
			put(Character.class, SQLiteType.TEXT);
			put(String.class, SQLiteType.TEXT);
			put(Byte[].class, SQLiteType.BLOB);
		}
	};

	//////////////////////////////////////////////////////////////////////////////////////
	// PUBLIC METHODS
	//////////////////////////////////////////////////////////////////////////////////////

	public static void execSql(String sql) {
		Cache.openDatabase().execSQL(sql);
	}

	public static void execSql(String sql, Object[] bindArgs) {
		Cache.openDatabase().execSQL(sql, bindArgs);
	}

	public static <T extends IModel> List<T> rawQuery(Class<? extends IModel> type, String sql, String[] selectionArgs) {
		Cursor cursor = Cache.openDatabase().rawQuery(sql, selectionArgs);
		List<T> entities = processCursor(type, cursor);
		cursor.close();

		return entities;
	}

	public static <T extends IModel> T rawQuerySingle(Class<? extends IModel> type, String sql, String[] selectionArgs) {
		List<T> entities = rawQuery(type, sql, selectionArgs);

		if (entities.size() > 0) {
			return entities.get(0);
		}

		return null;
	}

	// Database creation

	public static String createTableDefinition(TableInfo tableInfo) {
		final ArrayList<String> definitions = new ArrayList<String>();

		for (Field field : tableInfo.getFields()) {
			String definition = createColumnDefinition(tableInfo, field);
			if (!TextUtils.isEmpty(definition)) {
				definitions.add(definition);
			}
		}

        List<Field> primaryColumns = tableInfo.getPrimaryKeys();
        List<Field> foreignColumns = tableInfo.getForeignKeys();
        if(!primaryColumns.isEmpty()){
            StringBuilder builder = new StringBuilder("PRIMARY KEY(");


            List<Field> primaryNonAutoIncrement = new ArrayList<Field>();
            for(int i  =0 ; i< primaryColumns.size(); i++){
                PrimaryKey primaryKey = primaryColumns.get(i).getAnnotation(PrimaryKey.class);
                if(!primaryKey.type().equals(PrimaryKey.Type.AUTO_INCREMENT)) {
                    primaryNonAutoIncrement.add(primaryColumns.get(i));
                }
            }

            int primaryNonAutoSize = primaryNonAutoIncrement.size();
            for(int i = 0; i < primaryNonAutoSize; i++) {
                builder.append(tableInfo.getColumnName(primaryNonAutoIncrement.get(i)));
                if (i < primaryNonAutoSize - 1) {
                    builder.append(", ");
                }
            }

            if(primaryNonAutoSize>0) {
                builder.append(")");

                definitions.add(builder.toString());
            }
        }

        for(int i = 0; i < foreignColumns.size(); i++){
            final Field column = foreignColumns.get(i);
            ForeignKey foreignKey = column.getAnnotation(ForeignKey.class);

            StringBuilder forDef = new StringBuilder("FOREIGN KEY(");
            forDef.append(tableInfo.getColumnName(column)).append(") REFERENCES ")
                    .append(Cache.getTableName((Class<? extends IModel>) column.getType()))
                    .append("(").append(foreignKey.foreignColumn()).append(")");

            definitions.add(forDef.toString());
        }


		return String.format("CREATE TABLE IF NOT EXISTS %s (%s);", tableInfo.getTableName(),
				TextUtils.join(", ", definitions));
	}

	@SuppressWarnings("unchecked")
	public static String createColumnDefinition(TableInfo tableInfo, Field field) {
		StringBuilder definition = new StringBuilder();

		Class<?> type = field.getType();
		final String name = tableInfo.getColumnName(field);
		final TypeSerializer typeSerializer = Cache.getParserForType(field.getType());
		final Column column = field.getAnnotation(Column.class);

		if (typeSerializer != null) {
			type = typeSerializer.getSerializedType();
		}

		if (TYPE_MAP.containsKey(type)) {
			definition.append(name);
			definition.append(" ");
			definition.append(TYPE_MAP.get(type).toString());
		}
		else if (ReflectionUtils.isModel(type)) {
			definition.append(name);
			definition.append(" ");
			definition.append(SQLiteType.INTEGER.toString());
		}
		else if (ReflectionUtils.isSubclassOf(type, Enum.class)) {
			definition.append(name);
			definition.append(" ");
			definition.append(SQLiteType.TEXT.toString());
		}

		if (!TextUtils.isEmpty(definition)) {
			if (column.length() > -1) {
				definition.append("(");
				definition.append(column.length());
				definition.append(")");
			}

			if (field.isAnnotationPresent(PrimaryKey.class)) {
                PrimaryKey primaryKey = field.getAnnotation(PrimaryKey.class);
                if(primaryKey.type().equals(PrimaryKey.Type.AUTO_INCREMENT)){
				    definition.append(" PRIMARY KEY AUTOINCREMENT");
                }
			}

			if (column.notNull()) {
				definition.append(" NOT NULL ON CONFLICT ");
				definition.append(column.onNullConflict().toString());
			}

			if (column.unique()) {
				definition.append(" UNIQUE ON CONFLICT ");
				definition.append(column.onUniqueConflict().toString());
			}
		}
		else {
			AALog.e("No type mapping for: " + type.toString());
		}

		return definition.toString();
	}

	@SuppressWarnings("unchecked")
	public static <T extends IModel> List<T> processCursor(Class<? extends IModel> type, Cursor cursor) {
		final List<T> entities = new ArrayList<T>();

		try {
			Constructor<?> entityConstructor = type.getConstructor();

            //enable private constructors
            entityConstructor.setAccessible(true);

			if (cursor.moveToFirst()) {
				do {
					IModel entity = (T) entityConstructor.newInstance();
					entity.loadFromCursor(cursor);
					entities.add((T) entity);
				}
				while (cursor.moveToNext());
			}

		}
        catch (IllegalArgumentException i){
            throw new RuntimeException("Default constructor for: " + type.getName() + " was not found.");
        } catch (Exception e) {
			AALog.e("Failed to process cursor.", e);
		}

		return entities;
	}

    /**
     * Returns the where statement with primary keys with no values
     * @param tableInfo
     * @return
     */
    public static String getWhereStatement(Class<? extends IModel> modelClass, TableInfo tableInfo){
        List<Field> fields = new ArrayList<Field>();
        ArrayList<Field> primaryColumn = new ArrayList<Field>();
        fields = ReflectionUtils.getAllFields(fields, modelClass);

        for(Field field : fields){
            if(field.isAnnotationPresent(PrimaryKey.class)){
                primaryColumn.add(field);
            }
        }

        final StringBuilder where = new StringBuilder();
        for(int i = 0 ; i < primaryColumn.size(); i++){
            final Field field = primaryColumn.get(i);
            where.append(tableInfo.getColumnName(field));
            where.append("=?");

            if(i < primaryColumn.size()-1){
                where.append(" AND ");
            }
        }

        String sql = where.toString();

        return sql;
    }

    /**
     * Returns the where statement with primary keys and values filled in
     * @param IModel
     * @param tableInfo
     * @return
     */
    public static String getWhereStatement(IModel IModel, TableInfo tableInfo){
        List<Field> fields = new ArrayList<Field>();
        ArrayList<Field> primaryColumn = new ArrayList<Field>();
        fields = ReflectionUtils.getAllFields(fields, IModel.getClass());

        for(Field field : fields){
            if(field.isAnnotationPresent(PrimaryKey.class)){
                primaryColumn.add(field);
            }
        }

        final StringBuilder where = new StringBuilder();
        for(int i = 0 ; i < primaryColumn.size(); i++){
            final Field field = primaryColumn.get(i);
            where.append(tableInfo.getColumnName(field));
            where.append("=?");

            if(i < primaryColumn.size()-1){
                where.append(" AND ");
            }
        }

        String sql = where.toString();

        for(int i = 0; i < primaryColumn.size(); i++){
            final Field field = primaryColumn.get(i);
            field.setAccessible(true);
            try {
                Object object = field.get(IModel);
                if(object==null){
                    throw new PrimaryKeyCannotBeNullException("The primary key: " + field.getName() + "from " + tableInfo.getTableName() + " cannot be null.");
                } else {
                    final TypeSerializer typeSerializer = Cache.getParserForType(field.getType());
                    if (typeSerializer != null) {
                        // serialize data
                        object = typeSerializer.serialize(object);
                        // set new object type
                        if (object != null) {
                            Class fieldType = object.getClass();
                            // check that the serializer returned what it promised
                            if (!fieldType.equals(typeSerializer.getSerializedType())) {
                                AALog.w(String.format("TypeSerializer returned wrong type: expected a %s but got a %s",
                                        typeSerializer.getSerializedType(), fieldType));
                            }
                        }
                    }

                    if (object instanceof Number) {
                        sql = sql.replaceFirst("\\?", object.toString());
                    } else {
                        String escaped = DatabaseUtils.sqlEscapeString(object.toString());

                        sql = sql.replaceFirst("\\?", escaped);
                    }
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        return sql;
    }

    public static String getWhereFromEntityId(Class<? extends IModel> IModel, String entityId){
        String[] primaries = entityId.split(",");
        String whereString = getWhereStatement(IModel, Cache.getTableInfo(IModel));

        List<Field> fields = new ArrayList<Field>();
        fields = ReflectionUtils.getAllFields(fields, IModel);

        ArrayList<Field> primaryColumn = new ArrayList<Field>();
        for(Field field : fields){
            if(field.isAnnotationPresent(PrimaryKey.class)){
                primaryColumn.add(field);
            }
        }

        for(int i = 0; i < primaries.length; i++){
            final Field field = primaryColumn.get(i);
            field.setAccessible(true);
            try {
                if(field.getType().isAssignableFrom(String.class)){
                    String escaped = DatabaseUtils.sqlEscapeString(primaries[i]);
                    whereString = whereString.replaceFirst("\\?", escaped);
                } else {
                    whereString = whereString.replaceFirst("\\?", primaries[i]);
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        return whereString;
    }

    public static void delete(IModel IModel){
        TableInfo tableInfo = Cache.getTableInfo(IModel.getClass());
        Cache.openDatabase().delete(tableInfo.getTableName(), SQLiteUtils.getWhereStatement(IModel, tableInfo), null);
        Cache.removeEntity(IModel);

        Cache.getContext().getContentResolver()
                .notifyChange(ContentProvider.createUri(tableInfo.getType(), IModel.getId()), null);
    }


    /**
     *  {@link #save(com.activeandroid.IModel, int)} will check for the model to exist before inserting or updating
     */
    public static final int MODE_DEFAULT = 0;

    /**
     *  Used in {@link #save(com.activeandroid.IModel, int)}, we know the data is not new so we will update it all.
     *  If it does not do anything, we will insert it
     */
    public static final int MODE_UPDATE = 1;

    /**
     * We will only attempt to insert the data into the DB
     */
    public static final int MODE_INSERT = 3;


    /**
     * Saves the given {@link com.activeandroid.IModel} to the DB with {@link #MODE_DEFAULT}
     * @param iModel
     */
    public static void save(IModel iModel) {
        save(iModel, MODE_DEFAULT);
    }

    /**
     * Saves the given {@link com.activeandroid.IModel} to the DB.
     * @param IModel
     * @param mode
     */
    public static void save(IModel IModel, int mode){
        TableInfo tableInfo = Cache.getTableInfo(IModel.getClass());
        final SQLiteDatabase db = Cache.openDatabase();
        final ContentValues values = new ContentValues();

        for (Field field : tableInfo.getFields()) {
            String fieldName = tableInfo.getColumnName(field);
            Class<?> fieldType = field.getType();

            field.setAccessible(true);

            try {
                Object value = field.get(IModel);

                if (value != null) {
                    final TypeSerializer typeSerializer = Cache.getParserForType(fieldType);
                    if (typeSerializer != null) {
                        // serialize data
                        value = typeSerializer.serialize(value);
                        // set new object type
                        if (value != null) {
                            fieldType = value.getClass();
                            // check that the serializer returned what it promised
                            if (!fieldType.equals(typeSerializer.getSerializedType())) {
                                AALog.w(String.format("TypeSerializer returned wrong type: expected a %s but got a %s",
                                        typeSerializer.getSerializedType(), fieldType));
                            }
                        }
                    }
                }

                // TODO: Find a smarter way to do this? This if block is necessary because we
                // can't know the type until runtime.
                if (value == null) {
                    values.putNull(fieldName);
                }
                else if (fieldType.equals(Byte.class) || fieldType.equals(byte.class)) {
                    values.put(fieldName, (Byte) value);
                }
                else if (fieldType.equals(Short.class) || fieldType.equals(short.class)) {
                    values.put(fieldName, (Short) value);
                }
                else if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                    values.put(fieldName, (Integer) value);
                }
                else if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                    values.put(fieldName, (Long) value);
                }
                else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                    values.put(fieldName, (Float) value);
                }
                else if (fieldType.equals(Double.class) || fieldType.equals(double.class)) {
                    values.put(fieldName, (Double) value);
                }
                else if (fieldType.equals(Boolean.class) || fieldType.equals(boolean.class)) {
                    values.put(fieldName, (Boolean) value);
                }
                else if (fieldType.equals(Character.class) || fieldType.equals(char.class)) {
                    values.put(fieldName, value.toString());
                }
                else if (fieldType.equals(String.class)) {
                    values.put(fieldName, value.toString());
                }
                else if (fieldType.equals(Byte[].class) || fieldType.equals(byte[].class)) {
                    values.put(fieldName, (byte[]) value);
                }
                else if (field.isAnnotationPresent(ForeignKey.class) && ReflectionUtils.isModel(fieldType)) {
                    ForeignKey key = field.getAnnotation(ForeignKey.class);
                    if(!key.name().equals("")){
                        fieldName = field.getAnnotation(ForeignKey.class).name();
                    }
                    values.put(fieldName, ((IModel) value).getId());
                }
                else if (ReflectionUtils.isSubclassOf(fieldType, Enum.class)) {
                    values.put(fieldName, ((Enum<?>) value).name());
                }
            }
            catch (IllegalArgumentException e) {
                AALog.e(e.getClass().getName(), e);
            }
            catch (IllegalAccessException e) {
                AALog.e(e.getClass().getName(), e);
            }
        }

        boolean exists = false;
        if(mode == MODE_DEFAULT ) {
            exists = exists(IModel);
        } else if(mode == MODE_UPDATE) {
            exists = true;
        }

        if(exists) {
            exists = (db.update(tableInfo.getTableName(), values, SQLiteUtils.getWhereStatement(IModel, tableInfo), null) != 0);
        }

        if(!exists) {
            IModel.setRowId(db.insert(tableInfo.getTableName(), null, values));
            for(Field field : tableInfo.getPrimaryKeys()){
                if(field.isAnnotationPresent(PrimaryKey.class) &&
                        field.getAnnotation(PrimaryKey.class).type().equals(PrimaryKey.Type.AUTO_INCREMENT)){
                    field.setAccessible(true);
                    try {
                        field.set(IModel, IModel.getId());
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        Cache.getContext().getContentResolver()
                .notifyChange(ContentProvider.createUri(tableInfo.getType(), IModel.getId()), null);
    }

    public static void loadFromCursor(Cursor cursor, IModel IModel){
        TableInfo tableInfo = Cache.getTableInfo(IModel.getClass());
        for (Field field : tableInfo.getFields()) {
            final String fieldName = tableInfo.getColumnName(field);
            Class<?> fieldType = field.getType();
            final int columnIndex = cursor.getColumnIndex(fieldName);

            if (columnIndex < 0) {
                continue;
            }

            field.setAccessible(true);

            try {
                boolean columnIsNull = cursor.isNull(columnIndex);
                TypeSerializer typeSerializer = Cache.getParserForType(fieldType);
                Object value = null;

                if (typeSerializer != null) {
                    fieldType = typeSerializer.getSerializedType();
                }

                // TODO: Find a smarter way to do this? This if block is necessary because we
                // can't know the type until runtime.
                if (columnIsNull) {
                    field = null;
                }
                else if (fieldType.equals(Byte.class) || fieldType.equals(byte.class)) {
                    value = cursor.getInt(columnIndex);
                }
                else if (fieldType.equals(Short.class) || fieldType.equals(short.class)) {
                    value = cursor.getInt(columnIndex);
                }
                else if (fieldType.equals(Integer.class) || fieldType.equals(int.class)) {
                    value = cursor.getInt(columnIndex);
                }
                else if (fieldType.equals(Long.class) || fieldType.equals(long.class)) {
                    value = cursor.getLong(columnIndex);
                }
                else if (fieldType.equals(Float.class) || fieldType.equals(float.class)) {
                    value = cursor.getFloat(columnIndex);
                }
                else if (fieldType.equals(Double.class) || fieldType.equals(double.class)) {
                    value = cursor.getDouble(columnIndex);
                }
                else if (fieldType.equals(Boolean.class) || fieldType.equals(boolean.class)) {
                    value = cursor.getInt(columnIndex) != 0;
                }
                else if (fieldType.equals(Character.class) || fieldType.equals(char.class)) {
                    value = cursor.getString(columnIndex).charAt(0);
                }
                else if (fieldType.equals(String.class)) {
                    value = cursor.getString(columnIndex);
                }
                else if (fieldType.equals(Byte[].class) || fieldType.equals(byte[].class)) {
                    value = cursor.getBlob(columnIndex);
                }
                else if (field.isAnnotationPresent(ForeignKey.class) && ReflectionUtils.isModel(fieldType)) {
                    final String entityId = cursor.getString(columnIndex);
                    final Class<? extends IModel> entityType = (Class<? extends IModel>) fieldType;

                    IModel entity = Cache.getEntity(entityType, entityId);
                    if (entity == null) {
                        entity = new Select().from(entityType).where(SQLiteUtils.getWhereFromEntityId(entityType, entityId)).executeSingle();
                    }

                    value = entity;
                }
                else if (ReflectionUtils.isSubclassOf(fieldType, Enum.class)) {
                    @SuppressWarnings("rawtypes")
                    final Class<? extends Enum> enumType = (Class<? extends Enum>) fieldType;
                    value = Enum.valueOf(enumType, cursor.getString(columnIndex));
                }

                // Use a deserializer if one is available
                if (typeSerializer != null && !columnIsNull) {
                    value = typeSerializer.deserialize(value);
                }

                // Set the field name
                if (value != null) {
                    field.set(IModel, value);
                }
            }
            catch (IllegalArgumentException e) {
                AALog.e(e.getClass().getName(), e);
            }
            catch (IllegalAccessException e) {
                AALog.e(e.getClass().getName(), e);
            }
            catch (SecurityException e) {
                AALog.e(e.getClass().getName(), e);
            }
        }

        if (IModel.getId() != null) {
            Cache.addEntity(IModel);
        }
    }

    public static boolean exists(IModel iModel){
        IModel model = new Select().from(iModel.getClass()).where(SQLiteUtils.getWhereStatement(iModel, Cache.getTableInfo(iModel.getClass()))).executeSingle();
        return model!=null;
    }

}
