def get_emr_configuration():
    return {
        "Classification": "spark-log4j2",
        "Properties": {
            'log4j.rootCategory': 'WARN, console',
            'log4j.appender.console': 'org.apache.log4j.ConsoleAppender',
            'log4j.appender.console.layout': 'org.apache.log4j.PatternLayout',
            'log4j.appender.console.layout.ConversionPattern': '%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n',
            'log4j.appender.console.target': 'System.err',
        },
    }
