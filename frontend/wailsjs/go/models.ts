export namespace app {
	
	export class CroissantDownloadOption {
	    id: string;
	    group: string;
	    name: string;
	    fileName: string;
	    downloadUrl: string;
	    role: string;
	    accessMechanism: string;
	    accessLevel: string;
	    downstreamSystem: string;
	    contentSize: string;
	    license: string;
	    selectedByDefault: boolean;
	    supported: boolean;
	
	    static createFrom(source: any = {}) {
	        return new CroissantDownloadOption(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.id = source["id"];
	        this.group = source["group"];
	        this.name = source["name"];
	        this.fileName = source["fileName"];
	        this.downloadUrl = source["downloadUrl"];
	        this.role = source["role"];
	        this.accessMechanism = source["accessMechanism"];
	        this.accessLevel = source["accessLevel"];
	        this.downstreamSystem = source["downstreamSystem"];
	        this.contentSize = source["contentSize"];
	        this.license = source["license"];
	        this.selectedByDefault = source["selectedByDefault"];
	        this.supported = source["supported"];
	    }
	}
	export class CroissantDownloadPlan {
	    datasetName: string;
	    items: CroissantDownloadOption[];
	
	    static createFrom(source: any = {}) {
	        return new CroissantDownloadPlan(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.datasetName = source["datasetName"];
	        this.items = this.convertValues(source["items"], CroissantDownloadOption);
	    }
	
		convertValues(a: any, classs: any, asMap: boolean = false): any {
		    if (!a) {
		        return a;
		    }
		    if (a.slice && a.map) {
		        return (a as any[]).map(elem => this.convertValues(elem, classs));
		    } else if ("object" === typeof a) {
		        if (asMap) {
		            for (const key of Object.keys(a)) {
		                a[key] = new classs(a[key]);
		            }
		            return a;
		        }
		        return new classs(a);
		    }
		    return a;
		}
	}

}

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
	    storeName: string;
	
	    static createFrom(source: any = {}) {
	        return new UpdateInfo(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.available = source["available"];
	        this.latestVersion = source["latestVersion"];
	        this.url = source["url"];
	        this.storeName = source["storeName"];
	    }
	}

}

