export namespace main {
	
	export class SupportInfo {
	    appVersion: string;
	    osPlatform: string;
	    osVersion: string;
	
	    static createFrom(source: any = {}) {
	        return new SupportInfo(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.appVersion = source["appVersion"];
	        this.osPlatform = source["osPlatform"];
	        this.osVersion = source["osVersion"];
	    }
	}
	export class UpdateInfo {
	    available: boolean;
	    latestVersion: string;
	    url: string;
	
	    static createFrom(source: any = {}) {
	        return new UpdateInfo(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.available = source["available"];
	        this.latestVersion = source["latestVersion"];
	        this.url = source["url"];
	    }
	}

}

