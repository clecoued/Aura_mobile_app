/**
 * @file
 * @author  clecoued <clement.lecouedic@aura.healthcare>
 * @version 1.0
 *
 *
 * @section LICENSE
 *
 * Aura Mobile Application
 * Copyright (C) 2017 Aura Healthcare
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>
 *
 * @section DESCRIPTION
 *
 * LocalDataCouchbaseRepository is a local data storage implementation relying on Couchbase mobile
 * framework <https://developer.couchbase.com/documentation/mobile/current/installation/index.html>
 * The framework saves data in a NoSql database with a multiple documents architecture.
 * It provides good read/write performances, data encryption using sql_cipher library and automatic
 * syncing with remote database using Couchbase server (not implemented here).
 *
 */

package com.wearablesensor.aura.data_repository;

import android.content.Context;
import android.util.Log;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.Database;
import com.couchbase.lite.DatabaseOptions;
import com.couchbase.lite.Document;
import com.couchbase.lite.Emitter;
import com.couchbase.lite.Manager;
import com.couchbase.lite.Mapper;
import com.couchbase.lite.Query;
import com.couchbase.lite.QueryEnumerator;
import com.couchbase.lite.QueryRow;
import com.couchbase.lite.UnsavedRevision;
import com.couchbase.lite.View;
import com.couchbase.lite.android.AndroidContext;
import com.couchbase.lite.support.LazyJsonObject;
import com.wearablesensor.aura.data_repository.models.RRIntervalModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;


public class LocalDataCouchbaseRepository implements LocalDataRepository {
    private final String TAG = this.getClass().getSimpleName();

    private Manager mCouchBaseManager; /** couchbase manager */
    private Database mDB; /** local couchbase database */
    private DatabaseOptions mDBOptions; /** local couchbase database options */

    private View mRRSamplesView; /** pre-formatted view to query R-R interval on a date interval */

    private final static String DB_NAME = "dbaura"; /** couchbase database name */

    private final static String PHYSIO_SIGNAL_DOCUMENT= "physioSignalDocument"; /** document storing every physiological data */
    private final static String UUID_PARAM= "uuid"; /** UUID param used to map couchbase json to PhysioSignal model */
    private final static String TIMESTAMP_PARAM = "timestamp"; /** timestamp param used to map couchbase json to PhysioSignal model */
    private final static String USER_PARAM = "user"; /** user param used to map couchbase json to PhysioSignal model */
    private final static String RR_INTERVAL_PARAM = "rrInterval"; /** rrInterval param used to map couchbase json to PhysioSignal model */
    private final static String DEVICE_ADRESS_PARAM = "deviceAdress"; /** deviceAdress param used to map couchbase json to PhysioSignal model */


    private final static String RR_SAMPLES_VIEW = "rrSamplesView"; /** RR Sample view name */

    private ArrayList<RRIntervalModel> mRRSamplesCache; /* temporary storage in an array to avoid call overhead to local data repository */

    /**
     * @brief constructor
     *
     * @param iApplicationContext application context
     */

    public LocalDataCouchbaseRepository(Context iApplicationContext){
        Log.d(TAG, "Local data CouchBase repository init");
        mDBOptions = new DatabaseOptions();
        mDBOptions.setCreate(true);

        //TODO:implement encryption
        /*if (mEncryptionEnabled) {
            options.setEncryptionKey(key);
        }*/

        try {
            mCouchBaseManager = new Manager(new AndroidContext(iApplicationContext), Manager.DEFAULT_OPTIONS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            mDB = mCouchBaseManager.openDatabase(DB_NAME, mDBOptions);
        } catch (CouchbaseLiteException e) {
            e.printStackTrace();
        }

        // create Couchbase view used to query RRSamples
        mRRSamplesView = mDB.getView(RR_SAMPLES_VIEW);
        mRRSamplesView.setMap(new Mapper() {
            @Override
            public void map(Map<String, Object> document, Emitter emitter) {

                for (Map.Entry<String, Object> entry : document.entrySet())
                {
                    if(entry.getValue() instanceof LinkedHashMap) {
                        LinkedHashMap<String, Object> rr = (LinkedHashMap<String, Object>) entry.getValue();
                        if (rr.get(TIMESTAMP_PARAM) instanceof String) {
                            emitter.emit(DateIso8601Mapper.getDate((String) rr.get(TIMESTAMP_PARAM)), rr);
                        }
                    }
                }
            }
        },"1.0");

        mRRSamplesCache = new ArrayList<RRIntervalModel>();
    }

    /**
     * @brief query a list of R-R interval samples in a time range [iStartDate, iEndDate]
     *
     * @param iStartDate collected data sample timestamps are newer than iStartData
     * @param iEndDate collected data samples timestamp is older than iEndData
     *
     * @return a list of R-R interval samples
     *
     * @throws Exception
     */

    @Override
    public ArrayList<RRIntervalModel> queryRRSamples(Date iStartDate, Date iEndDate) throws Exception {
        Log.d(TAG, "start query RR Samples");
        Document lRrDocument = null;
        try {
            lRrDocument = mDB.getDocument(PHYSIO_SIGNAL_DOCUMENT);
            Log.d(TAG, "Get Document - id:" + lRrDocument.getId());
        }catch(Exception e){
            e.printStackTrace();
            throw e;
        }

        ArrayList<RRIntervalModel> lRrSamples = new ArrayList<RRIntervalModel>();

        try {
            Query query = mRRSamplesView.createQuery();
            query.setStartKey(iStartDate);
            query.setEndKey(iEndDate);
            QueryEnumerator queryEnum = query.run();
            Log.d(TAG,"Query size: " + queryEnum.getCount());

            for (Iterator<QueryRow> it = queryEnum; it.hasNext(); ) {
                QueryRow row=it.next();
                //TODO: implement a mapper to RRIntervalModel - need to understand why Couchbase does not systematically return same object type
                RRIntervalModel lRrSample;
                if(row.getValue() instanceof  LinkedHashMap) {
                    LinkedHashMap<String, Object> jsonMap = (LinkedHashMap<String, Object>) row.getValue();
                    lRrSample = new RRIntervalModel((String) jsonMap.get(UUID_PARAM), (String) jsonMap.get(DEVICE_ADRESS_PARAM), (String) jsonMap.get(USER_PARAM), (String) jsonMap.get(TIMESTAMP_PARAM), (Integer) jsonMap.get(RR_INTERVAL_PARAM));
                }
                else if(row.getValue() instanceof LazyJsonObject) {
                    LazyJsonObject jsonMap = (LazyJsonObject) row.getValue();
                    lRrSample = new RRIntervalModel((String) jsonMap.get(UUID_PARAM), (String) jsonMap.get(DEVICE_ADRESS_PARAM), (String) jsonMap.get(USER_PARAM), (String) jsonMap.get(TIMESTAMP_PARAM), (Integer) jsonMap.get(RR_INTERVAL_PARAM));
                }
                else{
                    lRrSample = (RRIntervalModel) row.getValue();
                }
                lRrSamples.add(lRrSample);
                Log.d(TAG,"Document contents: " + row.getKey() + " "+ row.getValue());
            }
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }

        Log.d(TAG, "Samples count - " + lRrSamples.size());
        return lRrSamples;
    }

    /**
     * @brief save a batch of R-R interval in the local storage
     *
     * @param iSamplesRR R-R interval list to be stored
     *
     * @throws Exception
     */
    @Override
    public void saveRRSamples(final ArrayList<RRIntervalModel> iSamplesRR) throws Exception{
        Document rrDocument = null;
        try {
            rrDocument = mDB.getDocument(PHYSIO_SIGNAL_DOCUMENT);
            Log.d(TAG, "Get Document - id:" + rrDocument.getId());
        }catch(Exception e){
            e.printStackTrace();
            throw e;
        }

        Log.d(TAG, "Start Recording - iSamples:" + iSamplesRR.size());

        try {
            rrDocument.update(new Document.DocumentUpdater() {
                @Override
                public boolean update(UnsavedRevision newRevision) {
                    Map<String, Object> properties = newRevision.getUserProperties();
                    for(int i = 0; i < iSamplesRR.size(); i++){
                        properties.put(iSamplesRR.get(i).getUuid(), iSamplesRR.get(i));
                    }

                    newRevision.setUserProperties(properties);
                    Log.d(TAG, "RecordSuccess");
                    return true;
                }
            });
        } catch (CouchbaseLiteException e) {
            Log.d(TAG, "RecordFail " + e.getMessage());
            throw e;
        }

        Log.d(TAG, "RecordHistory nbItems:" + rrDocument.getProperties().size());
    }

    /**
     * @brief cache an R-R interval in the heap and periodically clear cache and save data to local
     * storage
     *
     * @param iSampleRR R-R interval sample to be cached
     */
    public void cacheRRSample(RRIntervalModel iSampleRR) throws Exception{
        if(mRRSamplesCache.size() < 100) {
            mRRSamplesCache.add(iSampleRR);
        }
        else {
            try {
                clearCache();
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }

        }
    }

    /**
     * @brief clear cache and store data from heap to local data storage
     *
     */
    public void clearCache() throws Exception{
        saveRRSamples(mRRSamplesCache);
        mRRSamplesCache.clear();
    }

    /**
     * @brief clear entirely the local data storage
     */
    @Override
    public void clear() {
        Document lPhysioSignalDocument = null;

        try {
            lPhysioSignalDocument = mDB.getDocument(PHYSIO_SIGNAL_DOCUMENT);

        }catch(Exception e){
            e.printStackTrace();
        }

        try {
            lPhysioSignalDocument.delete();
            android.util.Log.d(TAG, "Documents deleted" );

        } catch (CouchbaseLiteException e) {
            e.printStackTrace();
        }
    }
}
