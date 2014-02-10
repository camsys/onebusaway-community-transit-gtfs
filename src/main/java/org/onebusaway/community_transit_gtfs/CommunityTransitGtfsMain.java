/**
 * Copyright (C) 2011 Brian Ferris <bdferris@onebusaway.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onebusaway.community_transit_gtfs;

import java.io.File;

import org.onebusaway.gtfs.model.calendar.ServiceDate;

public class CommunityTransitGtfsMain {


	public static void main(String[] args) throws Exception {

    if (args.length != 5 && args.length != 6) {
      usage();
      System.exit(-1);
    }

    CommunityTransitGtfsFactory factory = new CommunityTransitGtfsFactory();
    factory.setScheduleInputPath(new File(args[0]));
    factory.setGisInputPath(new File(args[1]));
    factory.setGtfsOutputPath(new File(args[2]));
    factory.setCalendarStartDate(parseDate(args[3]));
    factory.setCalendarEndDate(parseDate(args[4]));
    
    if( args.length == 4)
      factory.setModificationsPath(args[3]);

    factory.run();
  }

  private static ServiceDate parseDate(String s) throws Exception {
	  return ServiceDate.parseString(s);
  }
  
  private static void usage() {
    System.err.println("usage: schedule_path gis_path gtfs_path start_date end_date [modifications]");

  }
}
