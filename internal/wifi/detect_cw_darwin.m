#import <CoreWLAN/CoreWLAN.h>
#import <CoreLocation/CoreLocation.h>
#include <stdlib.h>

// WGLocationDelegate handles CLLocationManager authorization callbacks.
// Kept alive as a static so ARC doesn't release it.
@interface WGLocationDelegate : NSObject <CLLocationManagerDelegate>
@end
@implementation WGLocationDelegate
- (void)locationManagerDidChangeAuthorization:(CLLocationManager *)manager {
    // no-op — we only need the app to appear in Location Services list
}
// Legacy callback for macOS < 11
- (void)locationManager:(CLLocationManager *)manager
    didChangeAuthorizationStatus:(CLAuthorizationStatus)status {
}
@end

static CLLocationManager *gLocManager = nil;
static WGLocationDelegate *gLocDelegate = nil;

// cwRequestLocationAuthorization triggers the CoreLocation authorization flow
// so this .app bundle appears in System Settings → Location Services.
// Must be dispatched to the main thread; safe to call multiple times.
void cwRequestLocationAuthorization(void) {
    dispatch_async(dispatch_get_main_queue(), ^{
        if (gLocManager != nil) return;
        gLocDelegate = [[WGLocationDelegate alloc] init];
        gLocManager = [[CLLocationManager alloc] init];
        gLocManager.delegate = gLocDelegate;
        [gLocManager requestWhenInUseAuthorization];
    });
}

const char* cwCurrentSSID(void) {
    CWWiFiClient *client = [CWWiFiClient sharedWiFiClient];
    CWInterface *iface = [client interface];
    if (!iface) return NULL;
    NSString *ssid = [iface ssid];
    if (!ssid || ssid.length == 0) return NULL;
    return strdup([ssid UTF8String]);
}

const char* cwInterfaceName(void) {
    CWWiFiClient *client = [CWWiFiClient sharedWiFiClient];
    CWInterface *iface = [client interface];
    if (!iface) return NULL;
    NSString *name = [iface interfaceName];
    if (!name || name.length == 0) return NULL;
    return strdup([name UTF8String]);
}

// ---------- Event-driven SSID monitor ----------
//
// CoreWLAN exposes change notifications via the CWEventDelegate protocol.
// Subscribing to CWEventTypeSSIDDidChange / linkDidChange replaces the
// 5-second polling loop we previously used — the OS calls us only when
// the user actually moves between networks.
//
// Threading: Obj-C delegate callbacks fire on the main thread by default.
// We invoke the C function pointer that Go provided; Go does a non-blocking
// channel send. No locks, no allocations beyond the strdup'd c-string.

@interface WGSSIDDelegate : NSObject <CWEventDelegate>
@end

@implementation WGSSIDDelegate
- (void)ssidDidChangeForWiFiInterfaceWithName:(NSString *)interfaceName {
    extern void goSSIDChanged(const char *);
    CWWiFiClient *client = [CWWiFiClient sharedWiFiClient];
    CWInterface *iface = [client interfaceWithName:interfaceName];
    NSString *ssid = iface ? [iface ssid] : nil;
    const char *cstr = (ssid && ssid.length > 0) ? strdup([ssid UTF8String]) : strdup("");
    goSSIDChanged(cstr);
    // goSSIDChanged is responsible for free()ing — it copies into Go memory.
}

// linkDidChange also fires when the user joins/leaves networks. Treat it as
// an SSID-change signal so we don't miss transitions ssidDidChange skipped.
- (void)linkDidChangeForWiFiInterfaceWithName:(NSString *)interfaceName {
    [self ssidDidChangeForWiFiInterfaceWithName:interfaceName];
}
@end

static WGSSIDDelegate *gSSIDDelegate = nil;
static BOOL gSSIDMonitorActive = NO;

// cwStartSSIDMonitor subscribes the singleton delegate to CWWiFiClient
// SSID + link events. Returns 0 on success, non-zero (errno-like) on
// failure — caller (Go) falls back to polling if non-zero.
int cwStartSSIDMonitor(void) {
    __block int result = 0;
    dispatch_sync(dispatch_get_main_queue(), ^{
        if (gSSIDMonitorActive) return;
        CWWiFiClient *client = [CWWiFiClient sharedWiFiClient];
        if (!client) { result = 1; return; }
        if (!gSSIDDelegate) {
            gSSIDDelegate = [[WGSSIDDelegate alloc] init];
        }
        [client setDelegate:gSSIDDelegate];
        NSError *err = nil;
        [client startMonitoringEventWithType:CWEventTypeSSIDDidChange error:&err];
        if (err) { result = 2; return; }
        [client startMonitoringEventWithType:CWEventTypeLinkDidChange error:&err];
        if (err) { result = 3; return; }
        gSSIDMonitorActive = YES;
    });
    return result;
}

// cwStopSSIDMonitor tears down the subscription. Idempotent — safe to
// call when not active.
void cwStopSSIDMonitor(void) {
    dispatch_sync(dispatch_get_main_queue(), ^{
        if (!gSSIDMonitorActive) return;
        CWWiFiClient *client = [CWWiFiClient sharedWiFiClient];
        NSError *err = nil;
        [client stopMonitoringEventWithType:CWEventTypeSSIDDidChange error:&err];
        [client stopMonitoringEventWithType:CWEventTypeLinkDidChange error:&err];
        [client setDelegate:nil];
        gSSIDMonitorActive = NO;
    });
}
