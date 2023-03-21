package org.flywaydb.community.database.phoenixqueryserver;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.parser.Parser;
import org.flywaydb.core.internal.parser.ParsingContext;

public class PhoenixQueryServerParser extends Parser {
    protected PhoenixQueryServerParser(Configuration configuration, ParsingContext parsingContext) {
        super(configuration, parsingContext, 3);
    }
}