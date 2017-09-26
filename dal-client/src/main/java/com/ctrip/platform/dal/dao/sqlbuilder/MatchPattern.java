package com.ctrip.platform.dal.dao.sqlbuilder;

/**
 * The match pattern for like operation.
 * 
 * "head" will add % in front of the parameter; 
 * "tail" will add % behind the parameter;
 * "both" will add % at both end of the parameter;
 * 
 * Eg.
 * If the parameter is "abc", then header changed the 
 * final value in sql to "%abc"; tail changes it to "abc%"
 * and both changes it "%abc%"
 * 
 * @author jhhe
 *
 */
public enum MatchPattern {
    head,
    tail,
    both,
    none,
}
