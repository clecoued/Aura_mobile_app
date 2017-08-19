/**
 * @file DataSyncServiceObserver
 * @author  clecoued <clement.lecouedic@aura.healthcare>
 * @version 1.0
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
 * DataSyncServiceObserver is a parent class to be imherited by class listening to DataSyncService
 * events
 *
 */

package com.wearablesensor.aura.data_sync.notifications;

import java.util.Observable;
import java.util.Observer;

public class DataSyncServiceObserver implements Observer {
    private final String TAG = this.getClass().getSimpleName();

    @Override
    public void update(Observable o, Object arg) {
        if(arg instanceof DataSyncNotification){
            this.onDataSyncServiceNotification((DataSyncNotification) arg);
        }
    }

    /**
     * @brief method executed by observer class when receiving a data sync notification event
     *
     * @param iDataSyncNotification notification to be processed by observer class
     */
    public void onDataSyncServiceNotification(DataSyncNotification iDataSyncNotification){}
}