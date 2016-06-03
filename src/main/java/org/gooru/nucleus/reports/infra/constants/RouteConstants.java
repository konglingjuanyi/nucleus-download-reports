package org.gooru.nucleus.reports.infra.constants;

/**
 * Created by ashish on 26/4/16.
 */
public final class RouteConstants {

    private static final String API_VERSION = "v1";
    public static final String API_BASE_ROUTE = "/api/nucleus-download-reports/" + API_VERSION + '/';
    private static final String CLASS = "class";
    private static final String COURSE = "course";
    private static final char SLASH = '/';
    private static final char COLON = ':';
    private static final String DOWNLOAD = "download";
    private static final String REQUEST = "request";
    private static final String FILE = "file";
    
    public static final String CLASS_ID = "classId";
    public static final String COURSE_ID = "courseId";
    public static final String USER_ID = "userId";
    public static final String IS_TEACHER = "isTeacher";
    public static final String IS_STUDENT = "isStudent";
    public static final String USER_ROLE = "userRole";
    
    public static final String INTERNAL_METRICS_ROUTE = API_BASE_ROUTE + "internal/metrics";
    
    public static final String API_AUTH_ROUTE = API_BASE_ROUTE+"*";
    public static final String DOWNLOAD_REQUEST = API_BASE_ROUTE + CLASS + SLASH + COLON + CLASS_ID + SLASH + COURSE + SLASH + COLON + COURSE_ID + SLASH + DOWNLOAD + SLASH + REQUEST;
    public static final String DOWNLOAD_FILE =  API_BASE_ROUTE + CLASS + SLASH + COLON + CLASS_ID + SLASH + COURSE + SLASH + COLON + COURSE_ID + SLASH + DOWNLOAD + SLASH + FILE;

    private RouteConstants() {
        throw new AssertionError();
    }
}
