import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { AppComponent } from './app.component';
import { DownloadCardComponent } from './components/download-card/download-card.component';
import { ManifestDownloadCardComponent } from './components/manifest-download-card/manifest-download-card.component';

@NgModule({
  declarations: [
    AppComponent,
    DownloadCardComponent,
    ManifestDownloadCardComponent
  ],
  imports: [
    BrowserModule,
    FormsModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
